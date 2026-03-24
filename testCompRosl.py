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

N_FAILURE_CONSTANT    = [100]
EXPLORATION_SIZE      = 1000   
EXPLOITATION_SIZE     = [100]   
CSV_LIMITS            = [5000]

NUM_TRIALS            = 2
TRIAL_COLORS          = ['#1f77b4', '#d62728'] # Blue for Trial 1, Red for Trial 2

# ─── Run Flags ───────────────────────────────────────────────────────────────

RUN_ROSL_PAPER        = True
RUN_ROSL_POOLED       = True   

PRINT_PHASE_TABLE     = False

RESULTS_DIR = "resultsROSL"
os.makedirs(RESULTS_DIR, exist_ok=True)

# ─── File Utils & Pre-Processing ─────────────────────────────────────────────

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

def explode_multi_key(rows, key, delimiter=',', transform=None):
    result = []
    for row in rows:
        raw_val = row.get(key)
        if raw_val is None:
            raw_val = ''
            
        for val in raw_val.split(delimiter):
            val = val.strip()
            if transform:
                val = transform(val)
            if val:
                new_row = dict(row)
                new_row['_exploded_key'] = val
                result.append(new_row)
    return result

def imdbid_to_tconst(imdbid):
    try:
        return "tt" + str(int(imdbid)).zfill(7)
    except (ValueError, TypeError):
        return None

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
        "label":     "Actors joined with IMDB on knownForTitles",
        "file_r":    "data/movies/Actors.tsv",
        "key_r":     "_exploded_key",
        "display_r": ["nconst", "primaryName", "birthYear", "primaryProfession", "_exploded_key"],
        "file_s":    "data/movies/imdb.csv",
        "key_s":     "_tconst",
        "display_s": ["imdbid", "title", "year", "director"],
        "pre_r":     lambda rows: explode_multi_key(rows, "knownForTitles"),
        "pre_s":     lambda rows: [dict(list(r.items()) + [("_tconst", imdbid_to_tconst(r.get("imdbid")))]) for r in rows],
    },
    {
        "label":     "IMDB Cast joined with Actors on Name",
        "file_r":    "data/movies/imdb.csv",
        "key_r":     "_exploded_key",
        "display_r": ["title", "year", "_exploded_key"],
        "file_s":    "data/movies/Actors.tsv",
        "key_s":     "primaryName",
        "display_s": ["nconst", "primaryName", "birthYear", "primaryProfession"],
        "pre_r":     lambda rows: explode_multi_key(rows, "cast", transform=lambda x: x.split('(')[0].strip() if '(' in x else x.strip()),
        "pre_s":     None,
    },
]

# ─── Main Loop ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    for scenario in JOIN_SCENARIOS:
        label = scenario["label"]
        safe_label = label.replace(" ", "_").replace("/", "-")
        
        for limit in CSV_LIMITS:
            print(f"\n{'='*80}")
            print(f"  SCENARIO: {label} | LIMIT: {limit}")
            print(f"{'='*80}")
            
            # Load and Preprocess Data Once per Limit
            table_r_raw = load_csv(scenario["file_r"], limit=limit)
            table_s_raw = load_csv(scenario["file_s"], limit=limit)
            
            table_r = scenario["pre_r"](table_r_raw) if scenario.get("pre_r") else table_r_raw
            table_s = scenario["pre_s"](table_s_raw) if scenario.get("pre_s") else table_s_raw
                
            kr, ks = scenario["key_r"], scenario["key_s"]
            
            basic = StandardHashJoin(scenario["file_r"], kr, scenario["file_s"], ks)
            true_total = len(basic.join(table_r, table_s))
            print(f"  # Results per Standard Hash Join: {true_total}")

            for n_fail in N_FAILURE_CONSTANT:
                for explt_size in EXPLOITATION_SIZE:
                    exp_size = EXPLORATION_SIZE
                    
                    print(f"\n  -- Hyperparameters -> N_FAIL: {n_fail} | EXPLT: {explt_size} --")
                    
                    # Setup individual figure for this specific configuration
                    plt.figure(figsize=(10, 6))
                    
                    trial_results = []
                    
                    for trial in range(NUM_TRIALS):
                        print(f"    Running Trial {trial + 1}...")
                        color = TRIAL_COLORS[trial]
                        
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
                            
                        trial_results.append({
                            "trial_num": trial + 1,
                            "hist_paper": hist_paper, "hist_pool": hist_pool,
                            "stats_paper": (out_paper, est_paper, err_paper),
                            "stats_pool": (out_pool, est_pool, err_pool)
                        })

                        # ── Plotting for this Trial ──
                        
                        # Plot ROSL Paper (Dashed Line, Hollow Markers)
                        if hist_paper:
                            x = [h['out'] for h in hist_paper]
                            y = [h['err'] for h in hist_paper]
                            plt.plot(x, y, label=f"Trial {trial+1} (Paper)", color=color, linestyle='--', alpha=0.7)
                            
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
                            plt.plot(x, y, label=f"Trial {trial+1} (Pooled)", color=color, linestyle='-', alpha=0.9)
                            
                            x_exp = [h['out'] for h in hist_pool if h['phase'] == 'Exploration']
                            y_exp = [h['err'] for h in hist_pool if h['phase'] == 'Exploration']
                            plt.scatter(x_exp, y_exp, color=color, marker='o', s=45)
                            
                            x_explt = [h['out'] for h in hist_pool if h['phase'] == 'Exploitation']
                            y_explt = [h['err'] for h in hist_pool if h['phase'] == 'Exploitation']
                            plt.scatter(x_explt, y_explt, color=color, marker='s', s=45)

                    # ── Finalize Chart for this configuration ──
                    plt.xlabel("% Output (Progress)")
                    plt.ylabel("% Error (Estimation Accuracy)")
                    
                    plt.title(f"Scenario: {label}\nL: {limit} | Exp: {exp_size} | Explt: {explt_size} | N: {n_fail}")
                    
                    plt.grid(True, linestyle='--', alpha=0.5)
                    plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=9)
                    plt.tight_layout(rect=[0, 0.08, 0.82, 1]) 
                    
                    plt.figtext(0.05, 0.02,
                        "Circles: End of Exploration | Squares: End of Exploitation\n"
                        "Dashed Line: ROSL Paper | Solid Line: ROSL PooledEstimator",
                        fontsize=9
                    )
                    
                    # Save Chart
                    filename_suffix = f"{safe_label}_limit{limit}_exp{exp_size}_explt{explt_size}_nfail{n_fail}"
                    plot_path = os.path.join(RESULTS_DIR, f"chart_{filename_suffix}.png")
                    plt.savefig(plot_path)
                    print(f"    Saved chart to {plot_path}")
                    plt.close()

                    # ── Export CSV for this configuration ──
                    csv_path = os.path.join(RESULTS_DIR, f"table_{filename_suffix}.csv")
                    with open(csv_path, 'w', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            "Round", "Phase", 
                            "T1 Paper %O/%E", "T1 Pooled %O/%E", 
                            "T2 Paper %O/%E", "T2 Pooled %O/%E"
                        ])
                        
                        t1_paper = trial_results[0]["hist_paper"]
                        t1_pool  = trial_results[0]["hist_pool"]
                        t2_paper = trial_results[1]["hist_paper"]
                        t2_pool  = trial_results[1]["hist_pool"]
                        
                        max_rounds = max(len(t1_paper), len(t1_pool), len(t2_paper), len(t2_pool)) // 2
                        
                        for r in range(max_rounds):
                            for p_idx, phase_name in enumerate(["Exploration", "Exploitation"]):
                                idx = r * 2 + p_idx
                                csv_row = [r + 1, phase_name]
                                
                                # T1 Data
                                csv_row.append(f"{t1_paper[idx]['out']:.1f}% / {t1_paper[idx]['err']:.1f}%" if idx < len(t1_paper) else "-")
                                csv_row.append(f"{t1_pool[idx]['out']:.1f}% / {t1_pool[idx]['err']:.1f}%" if idx < len(t1_pool) else "-")
                                
                                # T2 Data
                                csv_row.append(f"{t2_paper[idx]['out']:.1f}% / {t2_paper[idx]['err']:.1f}%" if idx < len(t2_paper) else "-")
                                csv_row.append(f"{t2_pool[idx]['out']:.1f}% / {t2_pool[idx]['err']:.1f}%" if idx < len(t2_pool) else "-")
                                    
                                writer.writerow(csv_row)
                                
                    print(f"    Saved CSV to {csv_path}")

                    # ── Print Comparison Summary ──
                    for t_res in trial_results:
                        print(f"    -- Trial {t_res['trial_num']} Summary --")
                        s_paper = t_res["stats_paper"]
                        s_pool = t_res["stats_pool"]
                        if s_paper and s_paper[0] is not None:
                            print(f"      ROSL Paper           | Found: {s_paper[0]:<8} | Est: {s_paper[1]:<12.2f} | Error: {s_paper[2]:>7.2f}%")
                        if s_pool and s_pool[0] is not None:
                            print(f"      ROSL PooledEstimator | Found: {s_pool[0]:<8} | Est: {s_pool[1]:<12.2f} | Error: {s_pool[2]:>7.2f}%")