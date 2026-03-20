from ROSLJoinPaperIntent import ROSL as ROSL_PaperIntent

import csv
import os
import math
import random
import matplotlib.pyplot as plt
from typing import List, Dict

# Import the ROSL variants
# from HalfRandomActiveStateROSL import ROSL as ROSL_ActiveState
# from HalfRandomSurvivalWeightingROSL import ROSL as ROSL_Survival
from ROSLJoinPaper import ROSL as ROSL_Paper
from ROSLJoinPaperIntent import ROSL as ROSL_PaperIntent

from BasicJoin import BasicJoin as StandardHashJoin

# ─── Constants ───────────────────────────────────────────────────────────────

N_FAILURE_CONSTANT    = 100    
EXPLORATION_SIZE      = 100   
EXPLOITATION_SIZE     = 100   

CSV_LIMITS            = [1000, 5000, 10000]

# ─── Run Flags ───────────────────────────────────────────────────────────────

RUN_BASIC_HASH        = True
# RUN_ROSL_ActiveState  = True
# RUN_ROSL_SURVIVAL     = True
RUN_ROSL_PAPER        = True
RUN_ROSL_PAPER_INTENT = True   # NEW

PRINT_PHASE_TABLE     = False

RESULTS_DIR = "resultsROSL"
os.makedirs(RESULTS_DIR, exist_ok=True)

# ─── File Utils ──────────────────────────────────────────────────────────────

def find_file(filepath):
    if os.path.exists(filepath): return filepath
    directory = os.path.dirname(filepath) or "."
    basename = os.path.basename(filepath).lower()
    for fname in os.listdir(directory):
        if fname.lower() == basename:
            return os.path.join(directory, fname)
    raise FileNotFoundError(f"No file matching '{filepath}' found.")

def load_csv(filepath, limit=None):
    filepath = find_file(filepath)
    with open(filepath, newline='', encoding='utf-8') as f:
        sample = f.read(4096)
        f.seek(0)
        dialect = csv.excel
        try: dialect = csv.Sniffer().sniff(sample, delimiters='\t,')
        except: pass
        reader = csv.DictReader(f, dialect=dialect)
        rows = [row for i, row in enumerate(reader) if limit is None or i < limit]
    return rows

# ─── Instrumentation Logic ───────────────────────────────────────────────────

def run_instrumented_rosl(variant_name, rosl_class, table_r, table_s, kr, ks, true_total):
    rosl = rosl_class(EXPLORATION_SIZE, EXPLOITATION_SIZE, N_FAILURE_CONSTANT, "file_r", kr, "file_s", ks)
    rosl.size_r, rosl.size_s = len(table_r), len(table_s)
    
    results = []
    r_iter = iter(table_r)
    round_history = []
    round_idx = 1
    
    while True:
        S_iter = iter(table_s)

        # 1. Exploration
        rosl.in_exploitation = False
        rosl.exploration(r_iter, S_iter, results)
        if hasattr(rosl, 'update_estimate_exploration'):
            rosl.update_estimate_exploration()

        est_exp = rosl.estimate_join_size()
        round_history.append({
            'round': round_idx, 'phase': 'Exploration',
            'out': (len(results) / true_total * 100) if true_total else 0,
            'err': (abs(est_exp - true_total) / true_total * 100) if true_total else 0
        })

        # 2. Exploitation
        rosl.in_exploitation = True
        rosl.exploitation(r_iter, S_iter, results)

        est_explt = rosl.estimate_join_size()
        round_history.append({
            'round': round_idx, 'phase': 'Exploitation',
            'out': (len(results) / true_total * 100) if true_total else 0,
            'err': (abs(est_explt - true_total) / true_total * 100) if true_total else 0
        })

        rosl.round_number += 1
        rosl.agg_exploration_loaded = rosl.curr_exploration_loaded
        rosl.reset_between_rounds()

        if rosl.agg_exploration_loaded == 0:
            break

        round_idx += 1

    final_est = rosl.estimate_join_size()
    final_err = (abs(final_est - true_total) / true_total * 100) if true_total else float('inf')
        
    return round_history, len(results), final_est, final_err

# ─── Scenarios ───────────────────────────────────────────────────────────────

JOIN_SCENARIOS = [
    {
        "label": "IMDB self-join on director",
        "file_r": "data/movies/imdb.csv", "key_r": "director",
        "file_s": "data/movies/imdb.csv", "key_s": "director"
    }
]

# ─── Main Loop ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    for limit in CSV_LIMITS:
        print(f"\n{'='*80}\n  RUNNING WITH CSV_LIMIT = {limit}\n{'='*80}")
        
        for scenario in JOIN_SCENARIOS:
            table_r = load_csv(scenario["file_r"], limit=limit)
            table_s = load_csv(scenario["file_s"], limit=limit)
            kr, ks = scenario["key_r"], scenario["key_s"]
            
            basic = StandardHashJoin(scenario["file_r"], kr, scenario["file_s"], ks)
            true_total = len(basic.join(table_r, table_s))
            print(f"  Scenario: {scenario['label']} | # Results per Standard Hash Join: {true_total}")

            all_results = {}
            stats = {}

            # ROSL Paper
            if RUN_ROSL_PAPER:
                hist, out_count, est, err = run_instrumented_rosl(
                    "Paper", ROSL_Paper, table_r, table_s, kr, ks, true_total
                )
                all_results["ROSL Paper"] = hist
                stats["Original Paper ISPW"] = (out_count, est, err)
            
            # PaperIntent
            if RUN_ROSL_PAPER_INTENT:
                hist, out_count, est, err = run_instrumented_rosl(
                    "PaperIntent", ROSL_PaperIntent, table_r, table_s, kr, ks, true_total
                )
                all_results["ROSL PaperIntent"] = hist
                stats["Paper Intent ISPW"] = (out_count, est, err)

            # # Active State
            # if RUN_ROSL_ActiveState:
            #     hist, out_count, est, err = run_instrumented_rosl(
            #         "Active-State", ROSL_ActiveState, table_r, table_s, kr, ks, true_total
            #     )
            #     all_results["R-B ROSL*"] = hist
            #     stats["Active-State IPW"] = (out_count, est, err)

            # # Survival Weighting
            # if RUN_ROSL_SURVIVAL:
            #     hist, out_count, est, err = run_instrumented_rosl(
            #         "Survival", ROSL_Survival, table_r, table_s, kr, ks, true_total
            #     )
            #     all_results["S-W ROSL**"] = hist
            #     stats["Empirical Survival"] = (out_count, est, err)

            # ── Plotting ──
            plt.figure(figsize=(10, 6))
            colors = {
                'ROSL Paper': 'blue',
                'R-B ROSL*': 'red',
                'S-W ROSL**': 'green',
                'ROSL PaperIntent': 'purple'
            }
            
            for label, history in all_results.items():
                x = [h['out'] for h in history]
                y = [h['err'] for h in history]
                color = colors[label]
                plt.plot(x, y, label=label, color=color, alpha=0.6)
                
                x_exp = [h['out'] for h in history if h['phase'] == 'Exploration']
                y_exp = [h['err'] for h in history if h['phase'] == 'Exploration']
                plt.scatter(x_exp, y_exp, color=color, marker='o', s=30)
                
                x_explt = [h['out'] for h in history if h['phase'] == 'Exploitation']
                y_explt = [h['err'] for h in history if h['phase'] == 'Exploitation']
                plt.scatter(x_explt, y_explt, color=color, marker='s', s=45)

            plt.xlabel("% Output (Progress)")
            plt.ylabel("% Error (Estimation Accuracy)")
            plt.title(f"Accuracy vs Progress (Limit: {limit})")
            plt.grid(True, linestyle='--', alpha=0.5)
            plt.legend()
            plt.figtext(0.1, 0.01,
                "* R-B: 50/50 Random Exploration Cache & Active State Probe\n"
                "** S-W: 50/50 Random Exploration Cache & Empirical Survival Weighting",
                fontsize=8
            )
            
            plot_path = os.path.join(RESULTS_DIR, f"chart_limit_{limit}.png")
            plt.savefig(plot_path)
            print(f"  Saved chart to {plot_path}")

            # ── Terminal Output & CSV Export ──
            csv_path = os.path.join(RESULTS_DIR, f"table_limit_{limit}.csv")
            with open(csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Round", "Phase",
                    "ROSL Paper %O/%E",
                    "R-B ROSL* %O/%E",
                    "S-W ROSL** %O/%E",
                    "ROSL PaperIntent %O/%E"
                ])
                
                if PRINT_PHASE_TABLE:
                    print(f"\n{'Round':<6} | {'Phase':<12} | {'Paper %Output/%Error':<20} | {'R-B %O/%E':<15} | {'S-W %O/%E':<15} | {'Intent %O/%E':<15}")
                    print("-" * 100)
                
                max_rounds = max(len(h) for h in all_results.values()) // 2
                for r in range(max_rounds):
                    for p_idx, phase_name in enumerate(["Exploration", "Exploitation"]):
                        idx = r * 2 + p_idx
                        row_str = f"{r+1:<6} | {phase_name:<12}"
                        csv_row = [r + 1, phase_name]
                        
                        for algo in ["ROSL Paper", "R-B ROSL*", "S-W ROSL**", "ROSL PaperIntent"]:
                            if algo in all_results and idx < len(all_results[algo]):
                                h = all_results[algo][idx]
                                cell_str = f"{h['out']:.1f}% / {h['err']:.1f}%"
                                row_str += f" | {cell_str:<16}"
                                csv_row.append(cell_str)
                            else:
                                row_str += " |      -          "
                                csv_row.append("-")
                                
                        if PRINT_PHASE_TABLE:
                            print(row_str)
                        writer.writerow(csv_row)
                        
            print(f"\n  Saved detailed phase CSV table to {csv_path}")

            # Print Comparison Summary
            print("\n-- Estimate Comparison Summary " + "-" * 60)
            print(f"  # Results per Standard Hash Join: {true_total}")
            for variant, (out_count, est, err) in stats.items():
                print(f"  {variant:<20} | Found: {out_count:<8} | Est: {est:<12.2f} | Error: {err:>7.2f}%")
