import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Forces headless rendering
import matplotlib.pyplot as plt
import os
import itertools
import sys

def generate_combined_plots(input_dir='results', output_dir='plots'):
    os.makedirs(output_dir, exist_ok=True)

    # Define the groupings based on relation count
    relations_2 = ['Q9', 'Q10', 'Q11', 'Q12', 'Q15']
    relations_3 = ['Q2', 'Q3', 'Q5', 'Q8', 'Q9_3R', 'test']
    
    # Bundle the relation groups for the loop
    rel_groups = [
        ('2_Relations', relations_2),
        ('3_Relations', relations_3)
    ]
    
    # Experiment parameters
    sizes = ['01', '1', '10']
    z_vals = ['0', '1', '1_5']
    mems = ['64mb', '256mb']

    # Get the default Matplotlib color cycle
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

    count = 0
    # Automatically iterate through all 36 combinations
    for (rel_name, query_list), size, z, mem in itertools.product(rel_groups, sizes, z_vals, mems):
        fig, ax = plt.subplots(figsize=(10, 8))
        lines_plotted = 0
        color_idx = 0

        # Iterate only through the queries for this specific chart's relation group
        for q in query_list:
            filename = f"{q}_{size}g_z{z}_{mem}_sch1.csv"
            filepath = os.path.join(input_dir, filename)

            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath)
                    
                    if 'pct_output' in df.columns and 'time_sec' in df.columns:
                        df = df[(df['pct_output'] > 0) & (df['time_sec'] > 0)]
                        
                        if not df.empty:
                            # Reset index is critical here for the gap-stitching logic to work
                            df = df.sort_values('pct_output').reset_index(drop=True)
                            
                            label = f"{q}"
                            line_color = colors[color_idx % len(colors)]
                            color_idx += 1
                            
                            label_added = False
                            # Phase 1: Solid, Phase 2: Dotted, Phase 3: Dashed
                            styles = {1: '-', 2: ':', 3: '--'}
                            
                            for phase in sorted(df['phase'].dropna().unique()):
                                idx = df.index[df['phase'] == phase].tolist()
                                if not idx: continue
                                
                                # Stitch the gap: Connect to the last point of the previous phase
                                if min(idx) > 0:
                                    idx = [min(idx) - 1] + idx
                                    
                                phase_df = df.iloc[idx]
                                lbl = label if not label_added else None
                                
                                ax.plot(
                                    phase_df['pct_output'], 
                                    phase_df['time_sec'], 
                                    color=line_color, 
                                    linestyle=styles.get(phase, '-'), 
                                    marker='.', 
                                    markersize=8, 
                                    label=lbl, 
                                    alpha=0.85
                                )
                                label_added = True # Only add the label to the legend once per query
                                
                            lines_plotted += 1
                except Exception as e:
                    print(f"Error reading {filename}: {e}")

        # Only format and save the chart if at least one query line was successfully plotted
        if lines_plotted > 0:
            ax.set_xscale('log', base=10)
            ax.set_yscale('log', base=10)
            
            # Cap the right side of the X-axis at exactly 10%
            ax.set_xlim(right=10)
            
            ax.grid(True, which="both", linestyle='--', alpha=0.6)
            ax.set_xlabel('Output (%)', fontsize=12, fontweight='bold')
            ax.set_ylabel('Delay (seconds)', fontsize=12, fontweight='bold')
            
            plt.title(f'EHJ Performance: {rel_name} ({mem.upper()})\nDataset: tpch{size}g | Skew: Z={z}', fontsize=14, fontweight='bold')

            ax.legend(loc="upper left", fontsize='medium', framealpha=0.9)
            plt.tight_layout()
            
            out_path = os.path.join(output_dir, f"combined_{rel_name.lower()}_{size}g_z{z}_{mem}.png")
            
            # Explicitly overwrite at the start of execution
            open(out_path, 'w').close() 
            plt.savefig(out_path, dpi=300)
            print(f"Saved: {out_path} (Contains {lines_plotted} queries)")
            count += 1
            
        plt.close()
        
    print(f"\nSuccess! {count} combined charts generated in '{output_dir}/'.")

if __name__ == "__main__":
    target_dir = sys.argv[1] if len(sys.argv) > 1 else 'results'
    generate_combined_plots(input_dir=target_dir)