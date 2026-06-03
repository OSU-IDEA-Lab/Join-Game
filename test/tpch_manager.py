import subprocess
import itertools
from concurrent.futures import ThreadPoolExecutor
import sys
import os
import argparse

def get_queries(q_name, val, limit):
    """Returns a list of tuples: (SQL query, schema_val, num_relations) using the z{val} schema format."""
    sch_val = '1' 
    sql = ""
    num_relations = 0
    
    # Determine the number of relations upfront
    if q_name in ['Q9', 'Q10', 'Q11', 'Q12', 'Q15']:
        num_relations = 2
    elif q_name in ['Q2', 'Q3', 'Q5', 'Q8', 'Q9_3R', 'test']:
        num_relations = 3

    if limit:
        # 2R with output limits per paper as of 5/27/2026
        if q_name == 'Q9': sql = f"select * from z{val}.partsupp, z{val}.lineitem where ps_partkey = l_partkey LIMIT 221700;"
        elif q_name == 'Q10': sql = f"select * from z{val}.customer, z{val}.orders where c_custkey = o_custkey LIMIT 13000;"
        elif q_name == 'Q11': sql = f"select * from z{val}.orders, z{val}.lineitem where o_orderdate = l_shipdate LIMIT 327624700;"
        elif q_name == 'Q15': sql = f"select * from z{val}.supplier, z{val}.lineitem where s_suppkey = l_suppkey LIMIT 43800;"
        
        # 3R output limits to be gathered from paper
        elif q_name == 'Q2': sql = f"select * from z{val}.part, z{val}.supplier, z{val}.partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey LIMIT 100000;"
        elif q_name == 'Q3': sql = f"select * from z{val}.customer, z{val}.orders, z{val}.lineitem where c_custkey = o_custkey and o_orderkey = l_orderkey LIMIT 10000;"
        elif q_name == 'Q5': sql = f"select * from z{val}.orders, z{val}.supplier, z{val}.lineitem where s_suppkey = l_suppkey and o_orderkey = l_orderkey LIMIT 4000;"
        # elif q_name == 'Q8': sql = f"select * from z{val}.part, z{val}.supplier, z{val}.lineitem where p_partkey = l_partkey and s_suppkey = l_suppkey LIMIT ;"
        # elif q_name == 'Q9_3R': sql = f"select * from z{val}.supplier, z{val}.partsupp, z{val}.lineitem where s_suppkey = l_suppkey and ps_suppkey = l_suppkey LIMIT ;"
        # elif q_name == 'test': sql = f"select * from z{val}.part, z{val}.supplier, z{val}.lineitem where l_suppkey = s_suppkey and l_partkey = p_partkey LIMIT ;"
    
    else:
        # 2R without limits
        if q_name == 'Q9': sql = f"select * from z{val}.partsupp, z{val}.lineitem where ps_partkey = l_partkey;"
        elif q_name == 'Q10': sql = f"select * from z{val}.customer, z{val}.orders where c_custkey = o_custkey;"
        elif q_name == 'Q11': sql = f"select * from z{val}.orders, z{val}.lineitem where o_orderdate = l_shipdate;"
        elif q_name == 'Q12': sql = f"select * from z{val}.orders, z{val}.lineitem where o_orderkey = l_orderkey;"
        elif q_name == 'Q15': sql = f"select * from z{val}.supplier, z{val}.lineitem where s_suppkey = l_suppkey;"

        # 3R without limits
        elif q_name == 'Q2': sql = f"select * from z{val}.part, z{val}.supplier, z{val}.partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey;"
        elif q_name == 'Q3': sql = f"select * from z{val}.customer, z{val}.orders, z{val}.lineitem where c_custkey = o_custkey and o_orderkey = l_orderkey;"
        elif q_name == 'Q5': sql = f"select * from z{val}.orders, z{val}.supplier, z{val}.lineitem where s_suppkey = l_suppkey and o_orderkey = l_orderkey;"
        elif q_name == 'Q8': sql = f"select * from z{val}.part, z{val}.supplier, z{val}.lineitem where p_partkey = l_partkey and s_suppkey = l_suppkey;"
        elif q_name == 'Q9_3R': sql = f"select * from z{val}.supplier, z{val}.partsupp, z{val}.lineitem where s_suppkey = l_suppkey and ps_suppkey = l_suppkey;"
        elif q_name == 'test': sql = f"select * from z{val}.part, z{val}.supplier, z{val}.lineitem where l_suppkey = s_suppkey and l_partkey = p_partkey;"

    return [(sql, sch_val, num_relations)] if sql else []

def run_worker(cmd, log_out):
    """
    This is the blocking function that the thread pool runs. 
    It waits for the subprocess to finish completely before returning.
    """
    with open(log_out, 'w') as out_file:
        subprocess.run(cmd, stdout=out_file, stderr=subprocess.STDOUT)
    
    print(f"FINISHED: {log_out}", flush=True)

def manage(base_dir, apply_limits):
    sizes = ['10']
    vals = ['0', '1']
    work_mems = ['500MB']
    
    # 2R queries
    # queries = ['Q9', 'Q10', 'Q11', 'Q12', 'Q15']
    # 3R queries
    # queries = ['Q2', 'Q3', 'Q5', 'Q8', 'Q9_3R']
    # Paper queries
    queries = ['Q2', 'Q3', 'Q5', 'Q9', 'Q10', 'Q11', 'Q15']
    # All queries
    # queries = ['Q2', 'Q3', 'Q5', 'Q8', 'Q9', 'Q9_3R', 'Q10', 'Q11', 'Q12', 'Q15', 'test']

    time_limit = "3600"

    # Define the isolated output directories
    dir_2r = f"2r_{base_dir}" if base_dir else "2r_results_dir"
    dir_3r = f"3r_{base_dir}" if base_dir else "3r_results_dir"

    limit_status = "ON" if apply_limits else "OFF"
    print(f"Generating queue... Output Limits are {limit_status}")
    
    os.makedirs(dir_2r, exist_ok=True)
    os.makedirs(dir_3r, exist_ok=True)

    with ThreadPoolExecutor(max_workers=6) as executor:
        for size, z, mem, q in itertools.product(sizes, vals, work_mems, queries):
            variations = get_queries(q, z, apply_limits)        
            
            # Unpacking all 3 returned variables
            for sql, sch_val, num_relations in variations:
                
                # Route the output directory based on the relation count
                target_dir = dir_3r if num_relations == 3 else dir_2r
                
                log_out = os.path.join(target_dir, f"{q}_{size}g_z{z}_{mem}.nohup.log")
                
                cmd = [
                    'python3', 'test/worker.py', 
                    'tpch', f"{size}g", q, z, mem, time_limit, sch_val, target_dir, sql
                ]
                
                print(f"QUEUED: {q} | DB: tpch{size}g | Z: {z} | Mem: {mem} | Target: {target_dir}")
                executor.submit(run_worker, cmd, log_out)
                
    print("All benchmark tasks have completed.")

if __name__ == "__main__":
    # Setup argparse for boolean flags and optional directory strings
    parser = argparse.ArgumentParser(description="TPC-H Query Manager")
    parser.add_argument("target_dir", nargs='?', default="results", help="Base directory name (e.g., 'results' becomes '2r_results' and '3r_results')")
    parser.add_argument("--limit", action="store_true", help="Pass this flag to apply the LIMIT constraints to queries")
    
    args = parser.parse_args()
    
    manage(args.target_dir, args.limit)