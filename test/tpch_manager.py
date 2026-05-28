import subprocess
import itertools
from concurrent.futures import ThreadPoolExecutor

def get_queries(q_name, val):
    """Returns a list of tuples: (SQL query, schema_val) using the z{val} schema format."""
    sch_val = '1' 
    sql = ""
    # 2R
    if q_name == 'Q9': sql = f"select * from z{val}.partsupp, z{val}.lineitem where ps_partkey = l_partkey;"
    elif q_name == 'Q10': sql = f"select * from z{val}.customer, z{val}.orders where c_custkey = o_custkey;"
    elif q_name == 'Q11': sql = f"select * from z{val}.orders, z{val}.lineitem where o_orderdate = l_shipdate;"
    elif q_name == 'Q12': sql = f"select * from z{val}.orders, z{val}.lineitem where o_orderkey = l_orderkey;"
    elif q_name == 'Q15': sql = f"select * from z{val}.supplier, z{val}.lineitem where s_suppkey = l_suppkey;"

    # 2R with output limits per paper as of 5/27/2026
    # if q_name == 'Q9': sql = f"select * from z{val}.partsupp, z{val}.lineitem where ps_partkey = l_partkey LIMIT 221700;"
    # elif q_name == 'Q10': sql = f"select * from z{val}.customer, z{val}.orders where c_custkey = o_custkey LIMIT 13000;"
    # elif q_name == 'Q11': sql = f"select * from z{val}.orders, z{val}.lineitem where o_orderdate = l_shipdate LIMIT 327624700;"
    # elif q_name == 'Q15': sql = f"select * from z{val}.supplier, z{val}.lineitem where s_suppkey = l_suppkey LIMIT 43800;"

    # 3R
    # elif q_name == 'Q2': sql = f"select * from z{val}.part, z{val}.supplier, z{val}.partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey;"
    # elif q_name == 'Q3': sql = f"select * from z{val}.customer, z{val}.orders, z{val}.lineitem where c_custkey = o_custkey and o_orderkey = l_orderkey;"
    # elif q_name == 'Q5': sql = f"select * from z{val}.orders, z{val}.supplier, z{val}.lineitem where s_suppkey = l_suppkey and o_orderkey = l_orderkey;"
    # elif q_name == 'Q8': sql = f"select * from z{val}.part, z{val}.supplier, z{val}.lineitem where p_partkey = l_partkey and s_suppkey = l_suppkey;"
    # elif q_name == 'Q9_3R': sql = f"select * from z{val}.supplier, z{val}.partsupp, z{val}.lineitem where s_suppkey = l_suppkey and ps_suppkey = l_suppkey;"
    # elif q_name == 'test': sql = f"select * from z{val}.part, z{val}.supplier, z{val}.lineitem where l_suppkey = s_suppkey and l_partkey = p_partkey;"
    return [(sql, sch_val)] if sql else []

def run_worker(cmd, log_out):
    """
    This is the blocking function that the thread pool runs. 
    It waits for the subprocess to finish completely before returning.
    """
    with open(log_out, 'w') as out_file:
        # Notice we use subprocess.run here instead of Popen. 
        # .run() waits for the process to finish!
        subprocess.run(cmd, stdout=out_file, stderr=subprocess.STDOUT)
    
    print(f"FINISHED: {log_out}")

def manage():
    sizes = ['10']
    # vals = ['1_5']
    vals = ['0', '1']
    work_mems = ['500MB']
    queries = ['Q9', 'Q10', 'Q11','Q12','Q15']
    # queries = ['Q2', 'Q3', 'Q5', 'Q8', 'Q9', 'Q9_3R', 'Q10', 'Q11', 'Q12', 'Q15', 'test']
    time_limit = "3600"

    print("Generating queue and spinning up thread pool...")

    # Open the thread pool with a strict limit of 6 concurrent tasks
    with ThreadPoolExecutor(max_workers=6) as executor:
        
        for size, z, mem, q in itertools.product(sizes, vals, work_mems, queries):
            variations = get_queries(q, z)        
            
            for sql, sch_val in variations:
                log_out = f"results/{q}_{size}g_z{z}_{mem}.nohup.log"
                
                cmd = [
                    'python3', 'test/worker.py', 
                    'tpch', f"{size}g", q, z, mem, time_limit, sch_val, sql
                ]
                
                print(f"QUEUED: {q} | DB: tpch{size}g | Z: {z} | Mem: {mem}")
                
                # Hand the command to the thread pool queue
                executor.submit(run_worker, cmd, log_out)

    print("All benchmark tasks have completed.")

if __name__ == "__main__":
    manage()