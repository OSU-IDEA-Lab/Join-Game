import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Forces headless rendering
import matplotlib.pyplot as plt
import os
import itertools
import sys

def generate_combined_plots(input_dir):
    output_dir = "plots_"+input_dir
    os.makedirs(output_dir, exist_ok=True)

    # Define the groupings based on relation count
    relations_2 = ['Q9', 'Q10', 'Q11', 'Q12', 'Q15']
    relations_3 = ['Q2', 'Q3', 'Q5', 'Q8', 'Q9_3R', 'test']
    
    # Restrict to 10g dataset size
    sizes = ['10']
    z_vals = ['0', '1', '1_5']
    mems = ['64mb', '256mb']

    rel_groups = [
        ('2_Relations', relations_2),
        ('3_Relations', relations_3)
    ]

    # Map the phase codes to (linestyle, marker) tuples
    styles_map = {
        # 3-Relation (Lower Node, Upper Node)
        11: ('-', '.'),   # L1, U1: Solid, Point
        21: ('--', '.'),  # L2, U1: Dashed, Point
        31: (':', '.'),   # L3, U1: Dotted, Point
        12: ('-', '*'),   # L1, U2: Solid, Asterisk
        22: ('--', '*'),  # L2, U2: Dashed, Asterisk
        32: (':', '*'),   # L3, U2: Dotted, Asterisk
        13: ('-', 'h'),   # L1, U3: Solid, Hexagram
        23: ('--', 'h'),  # L2, U3: Dashed, Hexagram
        33: (':', 'h'),   # L3, U3: Dotted, Hexagram
        
        # Default fallbacks for 2-Relation (Single Node)
        1: ('-', '.'), 
        2: ('--', '.'), 
        3: (':', '.')
    }

    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
    count = 0

    # Iterate through all 12 chart configurations
    for (rel_name, query_list), mem, z in itertools.product(rel_groups, mems, z_vals):
        fig, ax = plt.subplots(figsize=(14, 8)) 
        lines_plotted = 0
        color_idx = 0
        size = sizes[0]

        # Iterate through the queries for this chart
        for q in query_list:
            filename = f"{q}_{size}g_z{z}_{mem}_sch1.csv"
            filepath = os.path.join(input_dir, filename)

            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath)
                    
                    if 'pct_output' in df.columns and 'time_sec' in df.columns:
                        df = df[(df['pct_output'] > 0) & (df['time_sec'] > 0)]
                        
                        if not df.empty:
                            df = df.sort_values('pct_output').reset_index(drop=True)
                            
                            # Label no longer needs size or skew since they are chart-wide
                            label = f"{q}"
                            line_color = colors[color_idx % len(colors)]
                            color_idx += 1
                            
                            label_added = False
                            
                            for phase in sorted(df['phase'].dropna().unique()):
                                idx = df.index[df['phase'] == phase].tolist()
                                if not idx: continue
                                
                                # Stitch the gap
                                if min(idx) > 0:
                                    idx = [min(idx) - 1] + idx
                                    
                                phase_df = df.iloc[idx]
                                lbl = label if not label_added else None
                                
                                # Fetch the requested line style and marker
                                ls, mk = styles_map.get(phase, ('-', '.'))
                                
                                ax.plot(
                                    phase_df['pct_output'], 
                                    phase_df['time_sec'], 
                                    color=line_color, 
                                    linestyle=ls, 
                                    marker=mk, 
                                    markersize=6, 
                                    label=lbl, 
                                    alpha=0.8
                                )
                                label_added = True
                                
                            lines_plotted += 1
                except Exception as e:
                    print(f"Error reading {filename}: {e}")

        if lines_plotted > 0:
            ax.set_xscale('log', base=10)
            ax.set_yscale('log', base=10)
            
            ax.set_xlim(right=10)
            
            ax.grid(True, which="both", linestyle='--', alpha=0.6)
            ax.set_xlabel('Output (%)', fontsize=12, fontweight='bold')
            ax.set_ylabel('Delay (seconds)', fontsize=12, fontweight='bold')
            
            # Title updated to reflect the specific z_val slice
            plt.title(f'Combined EHJ Performance: {rel_name} ({mem.upper()})\nDataset: tpch{size}g | Skew: Z={z}', fontsize=16, fontweight='bold')

            ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize='small', ncol=1)
            plt.tight_layout()
            
            # Output filename explicitly includes z_val mapping
            out_path = os.path.join(output_dir, f"combined_{rel_name.lower()}_{mem}_z{z}.png")
            
            open(out_path, 'w').close()
            plt.savefig(out_path, dpi=300, bbox_inches="tight")
            print(f"Saved: {out_path} (Contains {lines_plotted} configurations)")
            count += 1
            
        plt.close()
        
    print(f"\nSuccess! {count} split charts generated in '{output_dir}/'.")

if __name__ == "__main__":
    target_dir = sys.argv[1] if len(sys.argv) > 1 else 'results2'
    generate_combined_plots(input_dir=target_dir)