import subprocess
import itertools
from concurrent.futures import ThreadPoolExecutor
import sys
import os
import argparse

def get_queries(q_name, val, shuff, limit):
    """Returns a list of tuples: (SQL query, num_relations, target_pct) using the z{val}_shuff{shuff} schema format."""
    schema = f"z{val}_shuff{shuff}"
    sql = ""
    num_relations = 0
    
    # Determine the number of relations upfront
    if q_name in ['Q9', 'Q10', 'Q11', 'Q12', 'Q15']:
        num_relations = 2
    elif q_name in ['Q2', 'Q3', 'Q5', 'Q8', 'Q9_3R', 'test']:
        num_relations = 3
    else:
        target_pct = 1.00

    if limit:
        target_pct = 100.00

        # 2R with output limits per paper as of 5/27/2026
        if q_name == 'Q9': sql = f"select * from {schema}.partsupp, {schema}.lineitem where ps_partkey = l_partkey LIMIT 221700;"
        elif q_name == 'Q10': sql = f"select * from {schema}.customer, {schema}.orders where c_custkey = o_custkey LIMIT 13000;"
        elif q_name == 'Q11': sql = f"select * from {schema}.orders, {schema}.lineitem where o_orderdate = l_shipdate LIMIT 327624700;"
        elif q_name == 'Q15': sql = f"select * from {schema}.supplier, {schema}.lineitem where s_suppkey = l_suppkey LIMIT 43800;"
        elif q_name == 'Q12': sql = f"select * from {schema}.orders, {schema}.lineitem where o_orderkey = l_orderkey LIMIT 1000;"
        
        # 3R with output limits per paper as of 6/2/2026
        elif q_name == 'Q2': sql = f"select * from {schema}.part, {schema}.supplier, {schema}.partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey LIMIT 100000;"
        elif q_name == 'Q3': sql = f"select * from {schema}.customer, {schema}.orders, {schema}.lineitem where c_custkey = o_custkey and o_orderkey = l_orderkey LIMIT 10000;"
        elif q_name == 'Q5': sql = f"select * from {schema}.orders, {schema}.supplier, {schema}.lineitem where s_suppkey = l_suppkey and o_orderkey = l_orderkey LIMIT 4000;"

        # 3R without output limits, since querries were missing from SIGMOD paper as of 6/2/2026
        elif q_name == 'Q8': 
            sql = f"select * from {schema}.part, {schema}.supplier, {schema}.lineitem where p_partkey = l_partkey and s_suppkey = l_suppkey ;"
            target_pct = 1.00

        elif q_name == 'Q9_3R': 
            sql = f"select * from {schema}.supplier, {schema}.partsupp, {schema}.lineitem where s_suppkey = l_suppkey and ps_suppkey = l_suppkey ;"
            target_pct = 1.00

    else:
        target_pct = 1.00

        # 2R without limits
        if q_name == 'Q9': sql = f"select * from {schema}.partsupp, {schema}.lineitem where ps_partkey = l_partkey;"
        elif q_name == 'Q10': sql = f"select * from {schema}.customer, {schema}.orders where c_custkey = o_custkey;"
        elif q_name == 'Q11': sql = f"select * from {schema}.orders, {schema}.lineitem where o_orderdate = l_shipdate;"
        elif q_name == 'Q12': sql = f"select * from {schema}.orders, {schema}.lineitem where o_orderkey = l_orderkey;"
        elif q_name == 'Q15': sql = f"select * from {schema}.supplier, {schema}.lineitem where s_suppkey = l_suppkey;"

        # 3R without limits
        elif q_name == 'Q2': sql = f"select * from {schema}.part, {schema}.supplier, {schema}.partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey;"
        elif q_name == 'Q3': sql = f"select * from {schema}.customer, {schema}.orders, {schema}.lineitem where c_custkey = o_custkey and o_orderkey = l_orderkey;"
        elif q_name == 'Q5': sql = f"select * from {schema}.orders, {schema}.supplier, {schema}.lineitem where s_suppkey = l_suppkey and o_orderkey = l_orderkey;"
        elif q_name == 'Q8': sql = f"select * from {schema}.part, {schema}.supplier, {schema}.lineitem where p_partkey = l_partkey and s_suppkey = l_suppkey;"
        elif q_name == 'Q9_3R': sql = f"select * from {schema}.supplier, {schema}.partsupp, {schema}.lineitem where s_suppkey = l_suppkey and ps_suppkey = l_suppkey;"
        elif q_name == 'test': sql = f"select * from {schema}.part, {schema}.supplier, {schema}.lineitem where l_suppkey = s_suppkey and l_partkey = p_partkey;"

    return [(sql, num_relations, target_pct)] if sql else []

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
    zvals = ['0', '1']
    work_mems = ['500MB']
    shuffles = ['1', '2', '3']
    
    queries = ['Q2', 'Q3', 'Q5', 'Q8', 'Q9', 'Q9_3R', 'Q10', 'Q11', 'Q12', 'Q15']

    time_limit = "3600"

    limit_status = "ON" if apply_limits else "OFF"
    print(f"Generating queue... Output Limits are {limit_status}")
    
    base_name = base_dir if base_dir else "results"

    with ThreadPoolExecutor(max_workers=8) as executor:
        for size, z, mem, q, shuff in itertools.product(sizes, zvals, work_mems, queries, shuffles):
            
            variations = get_queries(q, z, shuff, apply_limits)        
            
            # Unpacking all 3 values now
            for sql, num_relations, target_pct in variations:
                
                prefix = "3r" if num_relations == 3 else "2r"
                target_dir = f"{prefix}_sch{shuff}_{base_name}"
                
                os.makedirs(target_dir, exist_ok=True)
                
                log_out = os.path.join(target_dir, f"{q}_{size}g_z{z}_{mem}_sch{shuff}.nohup.log")
                
                cmd = [
                    'python3', 'test/worker.py', 
                    'tpch', f"{size}g", q, z, mem, time_limit, shuff, target_dir, str(target_pct), sql
                ]
                
                print(f"QUEUED: {q} | DB: tpch{size}g | Z: {z} | Sch: {shuff} | Mem: {mem} | Target: {target_pct}% | Dir: {target_dir}")
                executor.submit(run_worker, cmd, log_out)
                
    print("All benchmark tasks have completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TPC-H Query Manager")
    parser.add_argument("target_dir", nargs='?', default="results", help="Base directory name")
    parser.add_argument("--limit", action="store_true", help="Pass this flag to apply the LIMIT constraints")
    args = parser.parse_args()
    
    manage(args.target_dir, args.limit)