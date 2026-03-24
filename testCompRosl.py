import csv
import os
import math
import random
import matplotlib.pyplot as plt
from typing import List, Dict

# Import the ROSL variants
from ROSLJoinPaper import ROSL as ROSL_Paper
from PooledEstROSL import ROSL as ROSL_PooledEstimator

from BasicJoin import BasicJoin as StandardHashJoin

# ─── Constants ───────────────────────────────────────────────────────────────

# N_FAILURE_CONSTANT    = [100, 1000]
N_FAILURE_CONSTANT    = [100, 200]    

EXPLORATION_SIZE      = 1000   
EXPLOITATION_SIZE     = [10, 100]   

# CSV_LIMITS            = [5000, 10000]
CSV_LIMITS            = [1000, 5000]


# ─── Run Flags ───────────────────────────────────────────────────────────────

RUN_BASIC_HASH        = True
RUN_ROSL_PAPER        = True
RUN_ROSL_POOLED       = True   

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

def run_instrumented_rosl(variant_name, rosl_class, table_r, table_s, kr, ks, true_total, exp_size, explt_size, n_fail):
    rosl = rosl_class(exp_size, explt_size, n_fail, "file_r", kr, "file_s", ks)
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
        "label":     "IMDB self-join on director",
        "file_r":    "data/movies/imdb.csv",
        "key_r":     "director",
        "display_r": ["imdbid", "title", "year", "director"],
        "file_s":    "data/movies/imdb.csv",
        "key_s":     "director",
        "display_s": ["imdbid", "title", "year", "director"],
        "pre_r":     None,
        "pre_s":     None,
    },
    {
        "label":     "Actors self-join on primaryProfession",
        "file_r":    "data/movies/Actors.tsv",
        "key_r":     "primaryProfession",
        "display_r": ["nconst", "primaryName", "primaryProfession"],
        "file_s":    "data/movies/Actors.tsv",
        "key_s":     "primaryProfession",
        "display_s": ["nconst", "primaryName", "primaryProfession"],
        "pre_r":     None,
        "pre_s":     None,
    },
    {
        "label":     "Actors joined with IMDB on knownForTitles = imdbid",
        "file_r":    "data/movies/Actors.tsv",
        "key_r":     "_exploded_key",
        "display_r": ["nconst", "primaryName", "birthYear", "primaryProfession", "_exploded_key"],
        "file_s":    "data/movies/imdb.csv",
        "key_s":     "_tconst",
        "display_s": ["imdbid", "title", "year", "director"],
        "pre_r":     lambda rows: explode_multi_key(rows, "knownForTitles"),
        "pre_s":     lambda rows: [dict(list(r.items()) + [("_tconst", imdbid_to_tconst(r.get("imdbid")))]) for r in rows],
    },
]

# A distinct color palette for different scenarios
SCENARIO_COLORS = ['#1f77b4', '#d62728', '#2ca02c', '#9467bd', '#ff7f0e', '#8c564b', '#e377c2']

# ─── Main Loop ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    for limit in CSV_LIMITS:
        for n_fail in N_FAILURE_CONSTANT:
            for explt_size in EXPLOITATION_SIZE:
                exp_size = EXPLORATION_SIZE
                
                # Setup the single figure for this hyperparameter set
                plt.figure(figsize=(12, 8))
                
                all_scenario_results = {}
                all_scenario_stats = {}
                
                for s_idx, scenario in enumerate(JOIN_SCENARIOS):
                    color = SCENARIO_COLORS[s_idx % len(SCENARIO_COLORS)]
                    label = scenario["label"]
                    
                    table_r = load_csv(scenario["file_r"], limit=limit)
                    table_s = load_csv(scenario["file_s"], limit=limit)
                    kr, ks = scenario["key_r"], scenario["key_s"]
                    
                    basic = StandardHashJoin(scenario["file_r"], kr, scenario["file_s"], ks)
                    true_total = len(basic.join(table_r, table_s))
                    print(f"\n{'='*80}")
                    print(f"\n  Scenario: {label} | # Results per Standard Hash Join: {true_total}")
                    print(f"  RUNNING WITH HYPERPARAMETERS:")
                    print(f"    CSV_LIMIT        = {limit}")
                    print(f"    EXPLORATION_SIZE = {exp_size}")
                    print(f"    EXPLOITATION_SIZE= {explt_size}")
                    print(f"    N_FAILURE_CONST  = {n_fail}")
                    print(f"{'='*80}")

                    hist_paper, out_paper, est_paper, err_paper = [], 0, 0.0, 0.0
                    hist_pool, out_pool, est_pool, err_pool = [], 0, 0.0, 0.0

                    # Run ROSL Paper
                    if RUN_ROSL_PAPER:
                        hist_paper, out_paper, est_paper, err_paper = run_instrumented_rosl(
                            "Paper", ROSL_Paper, table_r, table_s, kr, ks, true_total,
                            exp_size, explt_size, n_fail
                        )
                    
                    # Run ROSL PooledEstimator
                    if RUN_ROSL_POOLED:
                        hist_pool, out_pool, est_pool, err_pool = run_instrumented_rosl(
                            "PooledEstimator", ROSL_PooledEstimator, table_r, table_s, kr, ks, true_total,
                            exp_size, explt_size, n_fail
                        )
                        
                    # Store data for CSV and terminal summary
                    all_scenario_results[label] = {"Paper": hist_paper, "Pooled": hist_pool}
                    all_scenario_stats[label] = {
                        "TrueTotal": true_total,
                        "Paper": (out_paper, est_paper, err_paper),
                        "Pooled": (out_pool, est_pool, err_pool)
                    }

                    # ── Plotting logic for this specific scenario ──
                    
                    # Plot ROSL Paper (Dashed Line, Hollow Markers)
                    if hist_paper:
                        x = [h['out'] for h in hist_paper]
                        y = [h['err'] for h in hist_paper]
                        plt.plot(x, y, label=f"{label} (Paper)", color=color, linestyle='--', alpha=0.7)
                        
                        x_exp = [h['out'] for h in hist_paper if h['phase'] == 'Exploration']
                        y_exp = [h['err'] for h in hist_paper if h['phase'] == 'Exploration']
                        plt.scatter(x_exp, y_exp, color=color, marker='o', s=30, facecolors='none', edgecolors=color)
                        
                        x_explt = [h['out'] for h in hist_paper if h['phase'] == 'Exploitation']
                        y_explt = [h['err'] for h in hist_paper if h['phase'] == 'Exploitation']
                        plt.scatter(x_explt, y_explt, color=color, marker='s', s=30, facecolors='none', edgecolors=color)

                    # Plot ROSL PooledEstimator (Solid Line, Filled Markers)
                    if hist_pool:
                        x = [h['out'] for h in hist_pool]
                        y = [h['err'] for h in hist_pool]
                        plt.plot(x, y, label=f"{label} (Pooled)", color=color, linestyle='-', alpha=0.9)
                        
                        x_exp = [h['out'] for h in hist_pool if h['phase'] == 'Exploration']
                        y_exp = [h['err'] for h in hist_pool if h['phase'] == 'Exploration']
                        plt.scatter(x_exp, y_exp, color=color, marker='o', s=45)
                        
                        x_explt = [h['out'] for h in hist_pool if h['phase'] == 'Exploitation']
                        y_explt = [h['err'] for h in hist_pool if h['phase'] == 'Exploitation']
                        plt.scatter(x_explt, y_explt, color=color, marker='s', s=45)

                # ── Finalize Chart ──
                plt.xlabel("% Output (Progress)")
                plt.ylabel("% Error (Estimation Accuracy)")
                
                chart_title = (
                    f"Accuracy vs Progress\n"
                    f"Exploration Cache Limit: {exp_size} | Exploitation Cache Limit: {explt_size}\n"
                    f"N-Failure Constant: {n_fail} | Input Relation Size Limit: {limit}"
                )
                plt.title(chart_title)
                
                plt.grid(True, linestyle='--', alpha=0.5)
                # Position legend slightly outside the plot area if there are many scenarios
                plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left")
                
                # Adjust layout to fit legend and bottom text
                plt.tight_layout(rect=[0, 0.08, 0.85, 1]) 
                
                plt.figtext(0.05, 0.02,
                    "Circles: End of Exploration | Squares: End of Exploitation\n"
                    "Dashed Line: ROSL Paper | Solid Line: ROSL PooledEstimator",
                    fontsize=9
                )
                
                filename_suffix = f"limit{limit}_exp{exp_size}_explt{explt_size}_nfail{n_fail}"
                plot_path = os.path.join(RESULTS_DIR, f"chart_{filename_suffix}.png")
                plt.savefig(plot_path)
                print(f"\n  Saved chart to {plot_path}")
                plt.close()

                # ── Export Unified CSV ──
                csv_path = os.path.join(RESULTS_DIR, f"table_{filename_suffix}.csv")
                with open(csv_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["Scenario", "Round", "Phase", "ROSL Paper %O/%E", "ROSL PooledEstimator %O/%E"])
                    
                    for label, results_dict in all_scenario_results.items():
                        h_paper = results_dict.get("Paper", [])
                        h_pool = results_dict.get("Pooled", [])
                        max_rounds = max(len(h_paper), len(h_pool)) // 2
                        
                        for r in range(max_rounds):
                            for p_idx, phase_name in enumerate(["Exploration", "Exploitation"]):
                                idx = r * 2 + p_idx
                                csv_row = [label, r + 1, phase_name]
                                
                                if idx < len(h_paper):
                                    csv_row.append(f"{h_paper[idx]['out']:.1f}% / {h_paper[idx]['err']:.1f}%")
                                else:
                                    csv_row.append("-")
                                    
                                if idx < len(h_pool):
                                    csv_row.append(f"{h_pool[idx]['out']:.1f}% / {h_pool[idx]['err']:.1f}%")
                                else:
                                    csv_row.append("-")
                                    
                                writer.writerow(csv_row)
                                
                print(f"  Saved detailed unified CSV table to {csv_path}")

                # ── Print Comparison Summary ──
                print("\n-- Estimate Comparison Summary " + "-" * 60)
                for label, s in all_scenario_stats.items():
                    print(f"  Scenario: {label} (True Total: {s['TrueTotal']})")
                    if s.get("Paper") and s["Paper"][0] is not None:
                        print(f"    ROSL Paper       | Found: {s['Paper'][0]:<8} | Est: {s['Paper'][1]:<12.2f} | Error: {s['Paper'][2]:>7.2f}%")
                    if s.get("Pooled") and s["Pooled"][0] is not None:
                        print(f"    ROSL PooledEstimator | Found: {s['Pooled'][0]:<8} | Est: {s['Pooled'][1]:<12.2f} | Error: {s['Pooled'][2]:>7.2f}%")