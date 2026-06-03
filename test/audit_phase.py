import os
import glob
import pandas as pd
import sys

def check_3_relation_phases(results_dir='results'):
    # The known 3-relation queries
    queries_3r = ['Q2', 'Q3', 'Q5', 'Q8', 'Q9_3R', 'test']
    
    if not os.path.exists(results_dir):
        print(f"Error: Directory '{results_dir}' not found.")
        return

    search_pattern = os.path.join(results_dir, '*.csv')
    all_files = glob.glob(search_pattern)
    
    if not all_files:
        print(f"No CSV files found in '{results_dir}'.")
        return

    exceptions = []
    checked_count = 0
    
    for filepath in all_files:
        filename = os.path.basename(filepath)
        
        # Check if the file matches one of the 3-relation queries
        # Using + '_' ensures we don't accidentally match Q9 with Q9_3R
        is_3r = any(filename.startswith(q + '_') for q in queries_3r)
        if not is_3r:
            continue
            
        checked_count += 1
        
        try:
            df = pd.read_csv(filepath)
            if 'phase' not in df.columns:
                print(f"Warning: 'phase' column missing in {filename}")
                continue
            
            # Extract all unique phases present in the file
            phases = df['phase'].dropna().unique()
            
            # Find any phases that are strictly single digits (1, 2, or 3)
            single_digit_phases = [int(p) for p in phases if int(p) < 10]
            
            if single_digit_phases:
                exceptions.append({
                    'file': filename,
                    'invalid_phases': single_digit_phases,
                    'all_phases': [int(p) for p in phases]
                })
                
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            
    # --- Print Summary Report ---
    print("="*60)
    print("       3-Relation Phase Audit     ")
    print("="*60)
    print(f"Files Checked: {checked_count}")
    print(f"Exceptions Found (Nested Loops): {len(exceptions)}\n")
    
    if exceptions:
        print("--- EXCEPTIONS DETECTED ---")
        for exc in exceptions:
            print(f"File: {exc['file']}")
            print(f"  -> Single-Digit Phases Logged: {exc['invalid_phases']}")
            print(f"  -> All Phases in File:         {exc['all_phases']}\n")
    elif checked_count > 0:
        print("🎉 All checked 3-relation files successfully maintained 2-digit phases!")
        print("   (The EHJ pipeline remained stacked for all executions.)")

if __name__ == "__main__":
    # Allows passing an alternate directory, defaults to 'results'
    target_dir = sys.argv[1] if len(sys.argv) > 1 else 'results'
    check_3_relation_phases(target_dir)