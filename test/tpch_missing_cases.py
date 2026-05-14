import os
import itertools

def audit_results(results_dir='results'):
    # Define the exact parameters from your experiment matrix
    sizes = ['01', '1', '10']
    z_vals = ['0', '1', '1_5']
    mems = ['64mb', '256mb']
    queries = ['Q2', 'Q3', 'Q5', 'Q8', 'Q9', 'Q9_3R', 'Q10', 'Q11', 'Q12', 'Q15', 'test']
    schema = '1'

    expected_total = len(sizes) * len(z_vals) * len(mems) * len(queries)
    missing_files = []
    header_only_files = []

    if not os.path.exists(results_dir):
        print(f"Error: Directory '{results_dir}' not found.")
        return

    # Generate all 198 combinations
    for size, z, mem, query in itertools.product(sizes, z_vals, mems, queries):
        expected_filename = f"{query}_{size}g_z{z}_{mem}_sch{schema}.csv"
        expected_path = os.path.join(results_dir, expected_filename)
        
        # 1. Check if the file completely failed to generate
        if not os.path.exists(expected_path):
            missing_files.append({
                'query': query, 'size': f"{size}g", 'z': z, 'mem': mem
            })
        else:
            # 2. Check if the file only contains a header (1 line or less)
            try:
                with open(expected_path, 'r') as f:
                    # Read the lines to see how many there are
                    lines = f.readlines()
                    if len(lines) <= 1:
                        header_only_files.append({
                            'query': query, 'size': f"{size}g", 'z': z, 'mem': mem
                        })
            except Exception as e:
                print(f"Could not read {expected_filename}: {e}")

    # --- Print the Summary Report ---
    valid_count = expected_total - len(missing_files) - len(header_only_files)
    
    print(f"=====================================")
    print(f"       TPC-H Experiment Audit        ")
    print(f"=====================================")
    print(f"Expected CSVs:       {expected_total}")
    print(f"Valid Data CSVs:     {valid_count}")
    print(f"Missing CSVs:        {len(missing_files)}")
    print(f"Header-Only CSVs:    {len(header_only_files)}\n")

    # --- Print the Detailed Breakdowns ---
    if missing_files:
        print("--- COMPLETELY MISSING --- (Failed to start or save)")
        for item in missing_files:
            print(f"DB: tpch{item['size']:<3} | Z: {item['z']:<3} | Mem: {item['mem']:<5} | Query: {item['query']}")
        print("")

    if header_only_files:
        print("--- HEADER ONLY --- (Started, but crashed before finding matches)")
        for item in header_only_files:
            print(f"DB: tpch{item['size']:<3} | Z: {item['z']:<3} | Mem: {item['mem']:<5} | Query: {item['query']}")
        print("")

    if not missing_files and not header_only_files:
        print("🎉 All 198 CSV files are present and contain data!")

if __name__ == "__main__":
    audit_results()