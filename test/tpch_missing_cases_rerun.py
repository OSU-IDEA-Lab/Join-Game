import os
import itertools
import subprocess

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

def audit_results(results_dir='results'):
    sizes = ['01', '1', '10']
    z_vals = ['0', '1', '1_5']
    mems = ['64mb', '256mb']
    queries = ['Q2', 'Q3', 'Q5', 'Q8', 'Q9', 'Q9_3R', 'Q10', 'Q11', 'Q12', 'Q15', 'test']
    schema = '1'
    time_limit = "36000"

    expected_total = len(sizes) * len(z_vals) * len(mems) * len(queries)
    missing_files = []
    header_only_files = []

    if not os.path.exists(results_dir):
        print(f"Error: Directory '{results_dir}' not found.")
        return

    for size, z, mem, query in itertools.product(sizes, z_vals, mems, queries):
        # Updated to check for new naming convention: results/{q_name}_tpch{size}g_z{val}_{mem}_sch{sch_val}.csv
        expected_filename = f"{query}_tpch{size}g_z{z}_{mem}_sch{schema}.csv"
        expected_path = os.path.join(results_dir, expected_filename)
        
        if not os.path.exists(expected_path):
            missing_files.append({'query': query, 'size': size, 'z': z, 'mem': mem})
        else:
            try:
                with open(expected_path, 'r') as f:
                    lines = f.readlines()
                    if len(lines) <= 1:
                        header_only_files.append({'query': query, 'size': size, 'z': z, 'mem': mem})
            except Exception as e:
                print(f"Could not read {expected_filename}: {e}")

    valid_count = expected_total - len(missing_files) - len(header_only_files)
    print(f"--- TPC-H Audit: {valid_count}/{expected_total} Valid CSVs Found ---")

    if header_only_files:
        print("--- RELAUNCHING HEADER-ONLY CASES ---")
        for item in header_only_files:
            db_name = f"tpch{item['size']}g"
            variations = get_queries(item['query'], item['z'])
            for sql, sch_val in variations:
                log_out = f"results/rerun_{item['query']}_{db_name}_z{item['z']}_{item['mem']}.nohup.log"
                # Updated to pass db_name as the first worker argument
                cmd = ['nohup', 'python3', 'worker.py', db_name, item['query'], item['z'], item['mem'], time_limit, sch_val, sql]
                
                out_file = open(log_out, 'w')
                subprocess.Popen(cmd, stdout=out_file, stderr=subprocess.STDOUT, start_new_session=True)
                print(f"Launched: {db_name} | {item['query']} | Z:{item['z']} | {item['mem']}")

if __name__ == "__main__":
    audit_results()