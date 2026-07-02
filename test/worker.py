#!/bin/python
import sys
import datetime
import psycopg2
import csv
import re
from time import time
import os
import ground_truth  # shared JSON cache in project root

# Database connection details
USER = 'jinjo'
HOST = '/tmp/'
PORT = '1531'

# Parameters setting for the join algorithm
SIGMA = 0.99
DATA_LIMIT = 100
STEP_SIZE = 1.5
ITER_SIZE = 100

data_points = [int(ITER_SIZE * (STEP_SIZE ** i)) for i in range(100)]

# Fixed: Now accepts mem and time_limit dynamically
def get_mj_total(db_name, mem, time_limit, sql, z_val=None, dataset_size=None):
    # A SQL with LIMIT does not produce a valid ground-truth total, so the
    # cache is only used for uncapped queries. This matches the intent:
    # "record each run with no limit".
    cacheable = "limit" not in sql.lower() and z_val is not None and dataset_size is not None

    if cacheable:
        cached = ground_truth.lookup(sql, z_val, dataset_size)
        if cached is not None:
            print(f"CACHE HIT: total_tuples={cached} (skipped full count)", flush=True)
            return cached

    try:
        conn = psycopg2.connect(dbname=db_name, user=USER, host=HOST, port=PORT)
        conn.autocommit = False 
        with conn.cursor() as setup_cur:
            # Applies the exact work_mem and time_limit passed from the manager
            setup_cur.execute(f"SET work_mem = '{mem.upper()}'; SET statement_timeout = {time_limit * 1000};")
            setup_cur.execute("SET enable_hashjoin=off; SET enable_mergejoin=on; SET enable_nestloop=off;")
        conn.commit()
        with conn.cursor(name='mj_cur') as mj_cur:
            mj_cur.itersize = 2000
            mj_cur.execute(sql)
            total = sum(1 for _ in mj_cur)
        conn.close()

        if cacheable:
            ground_truth.store(sql, z_val, dataset_size, total)
            print(f"CACHE STORE: total_tuples={total}", flush=True)
        return total
    except Exception as e:
        # Fixed: Prints the error directly to the nohup.log file instead of swallowing it
        print(f"ERROR in get_mj_total: {e}", flush=True)
        return -1

def join_query(conn, server_cur, csv_writer, log_file, txt_file, total_tuples, time_limit, target_pct):
    current_phase, start_time, fetched_count, weighted_time = 1, time(), 0, 0
    prev_time, factor, idx = start_time, SIGMA, 0
    
    target_tuples = int(total_tuples * (target_pct / 100.0)) if total_tuples > 0 else float('inf')
    
    node_phases = {}
    rel_counts = {}
    
    for _ in server_cur:
        while conn.notices:
            notice = conn.notices.pop(0)
            log_file.write(f"SERVER INFO: {notice}\n")
            
            match = re.search(r"\[Node (\d+)\]", notice)
            
            new_p = None
            if "Starting Phase 1" in notice: new_p = 1
            elif "Entering Phase 2" in notice: new_p = 2
            elif "Entering Phase 3" in notice: new_p = 3
            
            if match:
                nid = int(match.group(1))
                if new_p: 
                    node_phases[nid] = new_p
                
                counts = re.search(r"outer\D*(\d+)\D*inner\D*(\d+)", notice, re.IGNORECASE)
                if counts:
                    if nid not in rel_counts: rel_counts[nid] = {'outer': 0, 'inner': 0}
                    rel_counts[nid]['outer'] = int(counts.group(1))
                    rel_counts[nid]['inner'] = int(counts.group(2))
            
            elif new_p:
                current_phase = new_p

        if node_phases:
            if len(node_phases) == 1:
                current_phase = list(node_phases.values())[0]
            else:
                upper_id = min(node_phases.keys())
                lower_id = max(node_phases.keys())
                current_phase = (node_phases[lower_id] * 10) + node_phases[upper_id]

        r1_raw, r2_raw, r3_raw = 0, 0, 0
        if rel_counts:
            if len(rel_counts) == 1: 
                nid = list(rel_counts.keys())[0]
                r1_raw = rel_counts[nid]['outer']
                r2_raw = rel_counts[nid]['inner']
            else: 
                lower_id = max(rel_counts.keys())
                upper_id = min(rel_counts.keys())
                r1_raw = rel_counts[lower_id]['outer']
                r2_raw = rel_counts[lower_id]['inner']
                r3_raw = rel_counts[upper_id]['inner']

        fetched_count += 1
        current_time = time()
        weighted_time += (current_time - prev_time) * factor
        prev_time, factor = current_time, factor * SIGMA
        
        if fetched_count % ITER_SIZE == 0:
            cumulative_time = current_time - start_time
            if fetched_count >= data_points[idx]:
                pct_output = (fetched_count / total_tuples) * 100 if total_tuples > 0 else 0.0
                
                csv_writer.writerow([fetched_count, round(cumulative_time, 4), current_phase, round(pct_output, 4), round(weighted_time, 4), r1_raw, r2_raw, r3_raw])
                txt_file.write(f"K val:{fetched_count}\tExecution time (unweighted): {cumulative_time:f}\tExecution time (weighted): {weighted_time:f}\n")
                
                idx += 1
            
            if cumulative_time >= time_limit or idx >= DATA_LIMIT: 
                if cumulative_time >= time_limit:
                    log_file.write(f"Timeout: {time_limit} sec reached.\n")
                break
                
        if fetched_count >= target_tuples: 
            break

    final_pct = round((fetched_count / total_tuples) * 100, 4) if total_tuples > 0 else 0.0
    csv_writer.writerow([fetched_count, round(time() - start_time, 4), current_phase, final_pct, round(weighted_time, 4), r1_raw, r2_raw, r3_raw])

def run_worker(dataset, dataset_size, q_name, z_val, mem, time_limit, sch_val, results_dir, target_pct, sql):
    time_limit = int(time_limit)
    target_pct = float(target_pct) 
    
    os.makedirs(results_dir, exist_ok=True)
    db_name = f"{dataset}{dataset_size}"
    
    # Fixed: Passes mem and time_limit to the total check
    # z_val + dataset_size enable the shared ground-truth cache (uncapped SQL only)
    total_tuples = get_mj_total(db_name, mem, time_limit, sql, z_val, dataset_size)
    if total_tuples < 0: return

    file_prefix = os.path.join(results_dir, f"{q_name}_{dataset_size}_z{z_val}_{mem.lower()}_sch{sch_val}")
    
    with open(f"{file_prefix}.log", 'w') as log_file, \
         open(f"{file_prefix}.csv", 'w', newline='') as f_csv, \
         open(f"{file_prefix}.txt", 'w') as txt_file:
         
        csv_writer = csv.writer(f_csv)
        csv_writer.writerow(['k_tuples', 'time_sec', 'phase', 'pct_output', 'weighted_sec', 'r1_raw', 'r2_raw', 'r3_raw'])
        
        txt_file.write(f"\tQuery: {sql}\n")
        
        conn = psycopg2.connect(dbname=db_name, user=USER, host=HOST, port=PORT)
        with conn.cursor() as setup_cur:
            setup_cur.execute(f"SET work_mem = '{mem.upper()}'; SET statement_timeout = {time_limit * 1000}; SET enable_hashjoin = ON; SET enable_mergejoin = OFF; SET enable_nestloop = OFF;")
        conn.commit()
        
        log_file.write(f"========================================================\n")
        log_file.write(f"Time: {datetime.datetime.now()} | Database: {db_name} | Query: {q_name}\n")
        log_file.write(f"Executing: {sql}\n")
        log_file.write(f"Total Merge Join Tuples (Baseline): {total_tuples}\n")
        log_file.write(f"Target Output Percentage: {target_pct}%\n")
        
        with conn.cursor(name='ehj_cursor') as sc:
            sc.itersize = ITER_SIZE
            sc.execute(sql)
            
            join_query(conn, sc, csv_writer, log_file, txt_file, total_tuples, time_limit, target_pct)
            
        conn.close()

        subject = f"Run Complete: {q_name} | DB: {db_name} | Z:{z_val} | {mem} | Sch:{sch_val}"
        body = f"The worker has finished processing {q_name} on {db_name} with Z={z_val}, Sch={sch_val}, and {mem} memory.\nData saved to: {results_dir}/"
        os.system(f'echo "{body}" | mail -s "{subject}" jinjo@oregonstate.edu')

if __name__ == "__main__":
    if len(sys.argv) == 11:
        run_worker(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9], sys.argv[10])
    else:
        print("Usage: python3 worker.py <dataset> <dataset_size> <q_name> <z_val> <mem> <time_limit_in_seconds> <sch_val> <results_dir> <target_pct> <sql_string>")