import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Forces headless rendering
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import os
import glob
import sys

def generate_plots_from_folder(input_dir='results', output_dir='plots'):
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Find all CSV files in the target folder
    csv_files = glob.glob(os.path.join(input_dir, '*.csv'))
    
    if not csv_files:
        print(f"Error: No CSV files found in the '{input_dir}' directory.")
        return

    # Expand colors for the 9 new states (Lower Node + Upper Node)
    phase_colors = {
        # 1-Node Configurations (2-Relation)
        1: '#0072BD', # Blue
        2: '#D95319', # Red
        3: '#77AC30', # Green
        
        # 2-Node Configurations (3-Relation) 
        11: '#1f77b4', # L1, U1 -> Blue
        12: '#17becf', # L1, U2 -> Cyan
        13: '#9467bd', # L1, U3 -> Purple
        21: '#ff7f0e', # L2, U1 -> Orange
        22: '#d62728', # L2, U2 -> Red
        23: '#8c564b', # L2, U3 -> Brown
        31: '#2ca02c', # L3, U1 -> Light Green
        32: '#bcbd22', # L3, U2 -> Olive
        33: '#333333'  # L3, U3 -> Black
    }
    
    # Legend label translation map
    phase_labels = {
        1: "Phase 1", 2: "Phase 2", 3: "Phase 3",
        11: "L1 + U1", 12: "L1 + U2", 13: "L1 + U3",
        21: "L2 + U1", 22: "L2 + U2", 23: "L2 + U3",
        31: "L3 + U1", 32: "L3 + U2", 33: "L3 + U3"
    }

    count = 0
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")
            continue

        if df.empty or 'k_tuples' not in df.columns or 'pct_output' not in df.columns:
            print(f"Skipping invalid or incomplete file: {csv_file}")
            continue

        basename = os.path.basename(csv_file).replace('.csv', '')

        fig, ax1 = plt.subplots(figsize=(10, 6))
        
        # Sort by k_tuples for smooth horizontal drawing
        df = df.sort_values('k_tuples')

        ax1.plot(df['k_tuples'], df['time_sec'], color='gray', alpha=0.3, zorder=1)

        # Plot individual node state scatters
        for phase in sorted(df['phase'].dropna().unique()):
            phase_data = df[df['phase'] == phase]
            color = phase_colors.get(phase, 'black')
            lbl = phase_labels.get(phase, f"Phase {int(phase)}")
            
            ax1.scatter(
                phase_data['k_tuples'],
                phase_data['time_sec'],
                c=color,
                s=60,
                label=lbl,
                zorder=2
            )

        max_k = df['k_tuples'].max()
        max_pct = df['pct_output'].max()
        ratio = (max_pct / max_k) if (max_k > 0 and max_pct > 0) else 0

        def combined_formatter(x, pos):
            pct = x * ratio
            return f"{x:,.0f}\n({pct:.1f}%)"

        ax1.xaxis.set_major_formatter(FuncFormatter(combined_formatter))

        ax1.grid(True, linestyle='--', alpha=0.6)
        ax1.set_xlabel('Results Produced (# and %)', fontsize=11, fontweight='bold', color='#333333')
        ax1.set_ylabel('Elapsed Time (seconds)', fontsize=11, fontweight='bold')
        
        plt.title(f'EHJ Phase Transitions: {basename}', fontsize=14, fontweight='bold')
        
        # Utilize ncol=2 if the legend gets too tall for the 3-relation joins
        legend_cols = 2 if len(df['phase'].unique()) > 4 else 1
        ax1.legend(loc='upper left', framealpha=0.9, ncol=legend_cols)
        plt.tight_layout()

        output_filename = os.path.join(output_dir, f"{basename}.png")
        plt.savefig(output_filename, dpi=300)
        plt.close() 
        
        count += 1
        print(f"Saved: {output_filename}")

    print(f"\nSuccess! {count} charts generated in the '{output_dir}/' directory.")

if __name__ == "__main__":
    target_dir = sys.argv[1] if len(sys.argv) > 1 else 'results'
    generate_plots_from_folder(input_dir=target_dir)