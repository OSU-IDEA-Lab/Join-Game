#!/bin/python
import sys
import datetime
import psycopg2
import csv
from time import time
import os

# Database connection details
USER = 'jinjo'
HOST = '/tmp/'
PORT = '1531'

# Parameters setting
SIGMA = .99
TIME_LIMIT = 3600               # 1 hr = 3600 seconds
DATA_LIMIT = 100                # Limit the number of data points
STEP_SIZE = 1.5                 # Data point step size
ITER_SIZE = 100                 # The number of rows per round trip check
TEST_NUMBER = 1
MAX_GLOBAL_RETRIES = 3

# Pre-calculate geometric data points
data_points = [int(ITER_SIZE * (STEP_SIZE ** i)) for i in range(100)]

def get_queries(q_name, val):
    """Returns a list of tuples: (SQL query, schema_val) using the z{val} schema format."""
    queries = []
    sch_val = '1' 
    sql = ""
    
    if q_name == 'Q9':
        sql = f"select * from z{val}.partsupp, z{val}.lineitem where ps_partkey = l_partkey LIMIT 240000;"
    elif q_name == 'Q10':
        sql = f"select * from z{val}.customer, z{val}.orders where c_custkey = o_custkey LIMIT 15000;"
    elif q_name == 'Q11':
        sql = f"select * from z{val}.orders, z{val}.lineitem where o_orderdate = l_shipdate LIMIT 13000000;"
    elif q_name == 'Q12':
        sql = f"select * from z{val}.orders, z{val}.lineitem where o_orderkey = l_orderkey LIMIT 60000;"
    elif q_name == 'Q15':
        sql = f"select * from z{val}.supplier, z{val}.lineitem where s_suppkey = l_suppkey LIMIT 60000;"
    elif q_name == 'Q2':
        sql = f"select * from z{val}.part, z{val}.supplier, z{val}.partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey LIMIT 8000;"
    elif q_name == 'Q3':
        sql = f"select * from z{val}.customer, z{val}.orders, z{val}.lineitem where c_custkey = o_custkey and o_orderkey = l_orderkey LIMIT 60000;"
    elif q_name == 'Q5':
        sql = f"select * from z{val}.orders, z{val}.supplier, z{val}.lineitem where s_suppkey = l_suppkey and o_orderkey = l_orderkey LIMIT 60000;"
    elif q_name == 'Q8':
        sql = f"select * from z{val}.part, z{val}.supplier, z{val}.lineitem where p_partkey = l_partkey and s_suppkey = l_suppkey LIMIT 60000;"
    elif q_name == 'Q9_3R':
        sql = f"select * from z{val}.supplier, z{val}.partsupp, z{val}.lineitem where s_suppkey = l_suppkey and ps_suppkey = l_suppkey LIMIT 4800000;"
    elif q_name == 'test':
        sql = f"select * from z{val}.part, z{val}.supplier, z{val}.lineitem where l_suppkey = s_suppkey and l_partkey = p_partkey LIMIT 60000;"
    
    if sql:
        queries.append((sql, sch_val))
        
    return queries

def get_mj_total(db_name, sql):
    """Runs a Merge Join baseline to get the true total row count for the query."""
    try:
        conn = psycopg2.connect(dbname=db_name, user=USER, host=HOST, port=PORT)
        # We must disable autocommit so the named cursor works
        conn.autocommit = False 
        
        with conn.cursor() as setup_cur:
            setup_cur.execute('SET enable_material=off;')
            setup_cur.execute('SET max_parallel_workers_per_gather=0;')
            setup_cur.execute('SET enable_hashjoin=off;')
            setup_cur.execute('SET enable_mergejoin=on;')
            setup_cur.execute('SET enable_indexonlyscan=off;')
            setup_cur.execute('SET enable_indexscan=off;')
            setup_cur.execute('SET enable_block=off;')
            setup_cur.execute('SET enable_bitmapscan=off;')
            setup_cur.execute('SET enable_seqscan=on;')
            setup_cur.execute('SET enable_nestloop=off;')
            setup_cur.execute("SET work_mem = '64kB';")
            setup_cur.execute('SET statement_timeout = 3600000;')
        conn.commit()
        
        # Fast iteration to count rows without storing them all in python memory
        with conn.cursor(name='mj_cur') as mj_cur:
            mj_cur.itersize = 2000
            mj_cur.execute(sql)
            total = sum(1 for _ in mj_cur)
            
        conn.close()
        return total
    except Exception as e:
        print(f"Error getting MJ baseline: {e}")
        if 'conn' in locals() and conn:
            conn.close()
        return -1

def join_query(conn, server_cur, csv_writer, log_file, total_tuples):
    """Executes the join, tracks EHJ phases, and logs data."""
    current_phase = 1  
    start_time = time()
    prev_time = start_time
    fetched_count = 0
    weighted_time = 0
    factor = SIGMA
    idx = 0
    result = []
    
    phase_map = {
        "Entering Phase 2": 2,
        "Entering Phase 3": 3,
        "Phase 1 complete": None 
    }

    for _ in server_cur:
        # Check EHJ phase transitions
        while conn.notices:
            notice = conn.notices.pop(0)
            log_file.write(f"SERVER INFO: {notice}\n")
            for key, val in phase_map.items():
                if key in notice and val is not None:
                    current_phase = val

        fetched_count += 1
        current_time = time()
        weighted_time += (current_time - prev_time) * factor
        prev_time = current_time
        factor *= SIGMA
        
        if fetched_count % ITER_SIZE == 0:
            cumulative_time = current_time - start_time
            if fetched_count >= data_points[idx]:
                log_file.write("%d, %f, %f\n" % (fetched_count, cumulative_time, weighted_time))
                result.append((fetched_count, cumulative_time, weighted_time))
                
                # Calculate % Output
                if total_tuples > 0:
                    pct_output = (fetched_count / total_tuples) * 100
                else:
                    pct_output = 0.0
                
                csv_writer.writerow([
                    fetched_count, round(cumulative_time, 4), current_phase, round(pct_output, 4)
                ])
                idx += 1
            
            if (cumulative_time >= TIME_LIMIT) or (idx >= DATA_LIMIT):
                if cumulative_time >= TIME_LIMIT:
                    log_file.write(f"Timeout: 1 hr reached.\n")
                break

    log_file.write("Total joined tuples fetched: %d\n" % fetched_count)
    log_file.write('Time of current query run: %.2f sec\n\n' % (time() - start_time))
    return result

def summary_tests(summary, test_results, Query):
    summary.write("\tQuery: %s\n" % Query)
    minLenRun = min(len(r) for r in test_results) if test_results else 0
    avg_test_results = {}
    
    for j in range(minLenRun):
        unweighted_sum = sum(res[j][1] for res in test_results)
        weighted_sum = sum(res[j][2] for res in test_results)
        tuples = test_results[0][j][0]
        
        avg_unw = unweighted_sum / len(test_results)
        avg_w = weighted_sum / len(test_results)
        
        summary.write("K val:%i\tTime (unweighted): %f\tTime (weighted): %f\n" % (tuples, avg_unw, avg_w))
        if tuples not in avg_test_results:
            avg_test_results[tuples] = {'unweighted': [], 'weighted': []}
        avg_test_results[tuples]['unweighted'].append(avg_unw)
        avg_test_results[tuples]['weighted'].append(avg_w)
    
    summary.write("\n")
    return avg_test_results

def run_experiment():
    sizes = ['01']
    # sizes = ['01', '1', '10']
    vals = ['0', '1', '1_5']
    # work_mems = ['64MB', '256MB']
    work_mems = ['64MB']
    all_queries = ['Q2', 'Q3', 'Q5', 'Q8', 'Q9', 'Q9_3R', 'Q10', 'Q11', 'Q12', 'Q15', 'test']

    # Ensure the output directory exists
    os.makedirs('results', exist_ok=True)

    for size in sizes:
        db_name = f"tpch{size}g"
        
        for val in vals:
            for q_name in all_queries:
                query_variations = get_queries(q_name, val)
                
                # 1. Run Merge Join Baseline exactly ONCE per unique query schema
                baselines = {}
                for sql, sch_val in query_variations:
                    print(f"\n[Baseline] {q_name} | DB: {db_name} | Z: {val} | Sch: {sch_val} -> Running Merge Join...")
                    total_tuples = get_mj_total(db_name, sql)
                    if total_tuples < 0:
                        print("    -> Error: Failed to get baseline. Skipping EHJ runs.")
                        continue
                    baselines[sch_val] = total_tuples
                    print(f"    -> Baseline complete. Total expected rows: {total_tuples}")

                # 2. Iterate through memory limits using the cached baseline
                for mem in work_mems:
                    file_prefix = f"results/{q_name}_{size}g_z{val}_{mem.lower()}"
                    result_log = f"{file_prefix}.log"
                    result_summary = f"{file_prefix}.txt"
                    result_data_unweight = f"{file_prefix}.dat"
                    result_data_weight = f"{file_prefix}_weight.dat"
                    
                    # Clear shared log/summary files
                    open(result_log, 'w').close()
                    
                    with open(result_summary, 'w') as summary, \
                         open(result_data_unweight, 'w') as data_unweight, \
                         open(result_data_weight, 'w') as data_weight:
                             
                        try:
                            conn = psycopg2.connect(dbname=db_name, user=USER, host=HOST, port=PORT)
                            conn.autocommit = False 
                            
                            with conn.cursor() as setup_cur:
                                setup_cur.execute(f"SET work_mem = '{mem}';")
                                setup_cur.execute("SET statement_timeout = 3600000;")
                                setup_cur.execute("SET enable_hashjoin = ON; SET enable_mergejoin = OFF;")
                                setup_cur.execute("SET enable_material = OFF; SET max_parallel_workers_per_gather = 0; SET enable_seqscan = ON; SET enable_indexonlyscan = OFF; SET enable_indexscan = OFF; SET enable_bitmapscan = OFF; SET enable_block = OFF; SET enable_fastjoin = OFF; SET enable_fliporder = OFF; SET enable_nestloop = ON;")
                            conn.commit()

                            avg_results = {}
                            
                            for sql, sch_val in query_variations:
                                # Skip if baseline failed
                                if sch_val not in baselines or baselines[sch_val] < 0:
                                    continue
                                
                                total_tuples = baselines[sch_val]
                                csv_filename = f"results/{file_prefix}_sch{sch_val}.csv"
                                
                                print(f"[Test Run] EHJ {q_name} | Mem: {mem} | DB: {db_name} | Z: {val} | Sch: {sch_val}")
                                
                                with open(result_log, 'a') as log_file, \
                                     open(csv_filename, 'w', newline='') as f_csv:
                                    
                                    csv_writer = csv.writer(f_csv)
                                    csv_writer.writerow(['k_tuples', 'time_sec', 'phase', 'pct_output'])
                                    
                                    log_file.write(f"========================================================\n")
                                    log_file.write(f"Time: {datetime.datetime.now()} | Query: {q_name} | Schema: {sch_val}\n")
                                    
                                    test_results = []
                                    for i in range(TEST_NUMBER):
                                        with conn.cursor(name='ehj_cursor') as sc:
                                            sc.itersize = ITER_SIZE
                                            sc.execute(sql)
                                            res = join_query(conn, sc, csv_writer, log_file, total_tuples)
                                            test_results.append(res)
                                            
                                    summary_res = summary_tests(summary, test_results, sql)
                                    # Union merge for simplicity across schema iterations
                                    for k, v in summary_res.items():
                                        if k not in avg_results:
                                            avg_results[k] = {'unweighted': [], 'weighted': []}
                                        avg_results[k]['unweighted'].extend(v['unweighted'])
                                        avg_results[k]['weighted'].extend(v['weighted'])
                            
                            # Write out the .dat files
                            summary.write("\n-- Average Execution Times Across Queries --\n")
                            for k in sorted(avg_results.keys()):
                                avg_unw = sum(avg_results[k]['unweighted']) / len(avg_results[k]['unweighted']) if avg_results[k]['unweighted'] else 0
                                avg_w = sum(avg_results[k]['weighted']) / len(avg_results[k]['weighted']) if avg_results[k]['weighted'] else 0
                                summary.write(f"K val:{k}\tAvg time (unweighted): {avg_unw}\tAvg time (weighted): {avg_w}\n")
                                data_unweight.write(f"{k}\t {avg_unw}\n")
                                data_weight.write(f"{k}\t {avg_w}\n")

                            conn.close()
                            
                        except Exception as e:
                            print(f"Error on {db_name} with {mem} running {q_name}: {e}")
                            if 'conn' in locals() and conn:
                                conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Usage: python script_name.py")
        sys.exit(1)
    run_experiment()