import subprocess
import itertools

def get_queries(q_name, val):
    """Returns a list of tuples: (SQL query, schema_val) using the z{val} schema format."""
    sch_val = '1' 
    sql = ""
    if q_name == 'Q9': sql = f"select * from z{val}.partsupp, z{val}.lineitem where ps_partkey = l_partkey LIMIT 240000;"
    elif q_name == 'Q10': sql = f"select * from z{val}.customer, z{val}.orders where c_custkey = o_custkey LIMIT 15000;"
    elif q_name == 'Q11': sql = f"select * from z{val}.orders, z{val}.lineitem where o_orderdate = l_shipdate LIMIT 13000000;"
    elif q_name == 'Q12': sql = f"select * from z{val}.orders, z{val}.lineitem where o_orderkey = l_orderkey LIMIT 60000;"
    elif q_name == 'Q15': sql = f"select * from z{val}.supplier, z{val}.lineitem where s_suppkey = l_suppkey LIMIT 60000;"
    elif q_name == 'Q2': sql = f"select * from z{val}.part, z{val}.supplier, z{val}.partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey LIMIT 8000;"
    elif q_name == 'Q3': sql = f"select * from z{val}.customer, z{val}.orders, z{val}.lineitem where c_custkey = o_custkey and o_orderkey = l_orderkey LIMIT 60000;"
    elif q_name == 'Q5': sql = f"select * from z{val}.orders, z{val}.supplier, z{val}.lineitem where s_suppkey = l_suppkey and o_orderkey = l_orderkey LIMIT 60000;"
    elif q_name == 'Q8': sql = f"select * from z{val}.part, z{val}.supplier, z{val}.lineitem where p_partkey = l_partkey and s_suppkey = l_suppkey LIMIT 60000;"
    elif q_name == 'Q9_3R': sql = f"select * from z{val}.supplier, z{val}.partsupp, z{val}.lineitem where s_suppkey = l_suppkey and ps_suppkey = l_suppkey LIMIT 4800000;"
    elif q_name == 'test': sql = f"select * from z{val}.part, z{val}.supplier, z{val}.lineitem where l_suppkey = s_suppkey and l_partkey = p_partkey LIMIT 60000;"
    return [(sql, sch_val)] if sql else []

def manage():
    sizes = ['01', '1', '10']
    vals = ['0', '1', '1_5']
    work_mems = ['64MB', '256MB']
    queries = ['Q2', 'Q3', 'Q5', 'Q8', 'Q9', 'Q9_3R', 'Q10', 'Q11', 'Q12', 'Q15', 'test']
    time_limit = "36000"

    for size, z, mem, q in itertools.product(sizes, vals, work_mems, queries):
        variations = get_queries(q, z)
        db_name = f"tpch{size}g"
        
        for sql, sch_val in variations:
            print(f"--- Launching: {q} | DB: {db_name} | Z: {z} | Mem: {mem} | Timeout: {time_limit}s ---")
            try:
                # Updated argument order: db_name is now first
                subprocess.run(['python3', 'worker.py', db_name, q, z, mem, time_limit, sch_val, sql], check=True)
            except subprocess.CalledProcessError as e:
                print(f"FAILED: {q} on {db_name} (Z={z}, Mem={mem}). Continuing to next...")

if __name__ == "__main__":
    manage()