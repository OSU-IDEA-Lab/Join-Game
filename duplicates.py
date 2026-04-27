import re

def analyze_duplicates(log_file="ehj_logs.txt", out_file="duplicates_report.txt"):
    matches = []
    
    # 1. Parse log file
    print(f"Parsing {log_file}...")
    with open(log_file, 'r') as f:
        for line in f:
            if "EHJ_MATCH:" in line:
                m = re.search(r"EHJ_MATCH: hash=(\d+) phase=(\d+) case=(.+)", line)
                if m:
                    hash_val = int(m.group(1))
                    phase = int(m.group(2))
                    case_val = m.group(3).strip()
                    matches.append((hash_val, phase, case_val))
                    
    if not matches:
        print("No matches found in the log file.")
        return
        
    print(f"Found {len(matches)} total emitted tuples.")
    
    # 2. Sort results so identical hash values are adjacent
    print("Sorting results by hash value...")
    matches.sort(key=lambda x: x[0])
    
    # 3. Linear scan to identify duplicates
    print("Scanning linearly for duplicates...")
    duplicates = []
    
    current_hash = None
    current_emissions = []
    
    for hash_val, phase, case_val in matches:
        if hash_val == current_hash:
            current_emissions.append((phase, case_val))
        else:
            if len(current_emissions) > 1:
                duplicates.append((current_hash, current_emissions))
            current_hash = hash_val
            current_emissions = [(phase, case_val)]
            
    # Catch the final group
    if len(current_emissions) > 1:
        duplicates.append((current_hash, current_emissions))
        
    # 4. Print results
    print(f"Found {len(duplicates)} duplicate tuples! Writing to {out_file}...")
    with open(out_file, 'w') as f:
        f.write(f"Total Duplicated Tuples Found: {len(duplicates)}\n")
        f.write("=" * 70 + "\n")
        for h, emissions in duplicates:
            emissions_str = " AND ".join([f"Phase {p} [{c}]" for p, c in emissions])
            f.write(f"Hash: {h:<10} | Emitted {len(emissions)} times: {emissions_str}\n")
            
    print("Done!")

if __name__ == "__main__":
    analyze_duplicates()