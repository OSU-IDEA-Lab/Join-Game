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

    # Define our phase colors
    phase_colors = {
        1: '#0072BD', # Blue
        2: '#D95319', # Red
        3: '#77AC30'  # Green
    }

    count = 0
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")
            continue

        # Skip empty CSVs
        if df.empty or 'k_tuples' not in df.columns or 'pct_output' not in df.columns:
            print(f"Skipping invalid or incomplete file: {csv_file}")
            continue

        # Extract the base name for the title and output file
        basename = os.path.basename(csv_file).replace('.csv', '')

        # Setup figure and primary axis
        fig, ax1 = plt.subplots(figsize=(10, 6))
        
        # --- SWAP: Sort by k_tuples for smooth horizontal drawing ---
        df = df.sort_values('k_tuples')

        # --- SWAP: Plot (X=Tuples, Y=Time) ---
        ax1.plot(df['k_tuples'], df['time_sec'], color='gray', alpha=0.3, zorder=1)

        # Overlay the colored scatter points to show the EHJ phases
        for phase in df['phase'].unique():
            if pd.isna(phase): 
                continue
                
            phase_data = df[df['phase'] == phase]
            color = phase_colors.get(phase, 'black')
            
            # --- SWAP: Scatter (X=Tuples, Y=Time) ---
            ax1.scatter(
                phase_data['k_tuples'],
                phase_data['time_sec'],
                c=color,
                s=60,
                label=f"Phase {int(phase)}",
                zorder=2
            )

        # Calculate the ratio for the custom tick labels
        max_k = df['k_tuples'].max()
        max_pct = df['pct_output'].max()
        ratio = (max_pct / max_k) if (max_k > 0 and max_pct > 0) else 0

        # Custom formatter function: takes tick value (x) and returns the multiline string
        def combined_formatter(x, pos):
            pct = x * ratio
            return f"{x:,.0f}\n({pct:.1f}%)"

        # --- SWAP: Apply the custom formatter to the X-axis instead of Y ---
        ax1.xaxis.set_major_formatter(FuncFormatter(combined_formatter))

        # --- SWAP: Update Labels ---
        ax1.grid(True, linestyle='--', alpha=0.6)
        ax1.set_xlabel('Results Produced (# and %)', fontsize=11, fontweight='bold', color='#333333')
        ax1.set_ylabel('Elapsed Time (seconds)', fontsize=11, fontweight='bold')
        
        plt.title(f'EHJ Phase Transitions: {basename}', fontsize=14, fontweight='bold')
        
        # Organize the legend
        ax1.legend(loc='upper left', framealpha=0.9)
        plt.tight_layout()

        # Save and close
        output_filename = os.path.join(output_dir, f"{basename}.png")
        plt.savefig(output_filename, dpi=300)
        plt.close() 
        
        count += 1
        print(f"Saved: {output_filename}")

    print(f"\nSuccess! {count} charts generated in the '{output_dir}/' directory.")

if __name__ == "__main__":
    target_dir = sys.argv[1] if len(sys.argv) > 1 else 'results'
    generate_plots_from_folder(input_dir=target_dir)