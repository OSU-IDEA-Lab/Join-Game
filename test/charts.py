import os
import sys
import glob
import pandas as pd
import matplotlib.pyplot as plt

def generate_charts(results_dir):
    if not os.path.exists(results_dir):
        print(f"Error: Directory '{results_dir}' not found.")
        return

    # Create a subfolder for plots inside the results directory
    plots_dir = os.path.join(results_dir, 'plots')
    os.makedirs(plots_dir, exist_ok=True)

    search_pattern = os.path.join(results_dir, '*.csv')
    csv_files = glob.glob(search_pattern)
    
    if not csv_files:
        print(f"No CSV files found in '{results_dir}'.")
        return

    print(f"Found {len(csv_files)} CSV files. Generating charts...")
    success_count = 0

    for file in csv_files:
        filename = os.path.basename(file)
        query_name = filename.replace('.csv', '')
        
        try:
            df = pd.read_csv(file)
            
            # Verify the CSV has our updated 4-column layout
            if not all(col in df.columns for col in ['time_sec', 'phase', 'pct_output']):
                print(f"Skipping {filename} - missing required columns.")
                continue

            fig, ax1 = plt.subplots(figsize=(10, 6))

            # --- PLOT 1: Percentage Output (Blue Line) ---
            color = 'tab:blue'
            ax1.set_xlabel('Time (Seconds)', fontweight='bold')
            ax1.set_ylabel('Tuple Output %', color=color, fontweight='bold')
            ax1.plot(df['time_sec'], df['pct_output'], color=color, linewidth=2.5)
            ax1.tick_params(axis='y', labelcolor=color)
            ax1.grid(True, linestyle='--', alpha=0.6)
            
            # Auto-scale the X and Y axes to fit the data perfectly (fixes the 1% squish)
            max_time = df['time_sec'].max()
            max_pct = df['pct_output'].max()
            ax1.set_xlim(0, max_time * 1.05 if max_time > 0 else 1)
            ax1.set_ylim(0, max_pct * 1.05 if max_pct > 0 else 1)

            # --- PLOT 2: Phase Transitions (Red Stepped Line) ---
            ax2 = ax1.twinx()
            color = 'tab:red'
            ax2.set_ylabel('Pipeline Phase', color=color, fontweight='bold')
            
            # Use a 'post' step plot so phase changes look like distinct hardware states
            ax2.step(df['time_sec'], df['phase'], color=color, where='post', linewidth=2, linestyle='--')
            ax2.tick_params(axis='y', labelcolor=color)
            
            # Dynamically set Y-ticks to only show the exact phases that occurred
            phases = sorted(df['phase'].dropna().unique())
            padding = max(1, (max(phases) - min(phases)) * 0.2) if len(phases) > 1 else 2
            ax2.set_ylim(min(phases) - padding, max(phases) + padding)
            ax2.set_yticks(phases)

            plt.title(f"Execution Profile: {query_name}", fontsize=14, pad=15)
            fig.tight_layout()

            # Save the plot
            plot_path = os.path.join(plots_dir, f"{query_name}.png")
            plt.savefig(plot_path, dpi=150)
            plt.close()
            
            success_count += 1
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")

    print(f"🎉 Successfully generated {success_count} charts in '{plots_dir}'.")

if __name__ == "__main__":
    # Accept target directory as an argument, default to 'results'
    target_dir = sys.argv[1] if len(sys.argv) > 1 else 'results'
    generate_charts(target_dir)