import os

def generate_latex_file(plots_dir='plots', output_file='overleaf_figures.txt'):
    # Ensure the plots directory exists before reading
    if not os.path.exists(plots_dir):
        print(f"Error: {plots_dir} directory not found.")
        return

    # Get all PNG files produced by your charting script
    files = sorted([f for f in os.listdir(plots_dir) if f.endswith('.png')])
    
    if not files:
        print("No charts found in the plots directory.")
        return

    with open(output_file, 'w') as f_out:
        f_out.write("% --- LaTeX Appendix for EHJ Benchmarks ---\n\n")
        
        for f in files:
            # Format the caption for readability (e.g., Q12 10g z1.5 64mb)
            caption = f.replace('_', ' ').replace('.png', '').replace('sch1', '').strip()
            
            # Using the [H] placement requires \usepackage{float} in your preamble
            f_out.write("\\begin{figure}[H]\n")
            f_out.write("    \\centering\n")
            f_out.write(f"    \\includegraphics[width=0.8\\textwidth]{{plots/{f}}}\n")
            f_out.write(f"    \\caption{{{caption}}}\n")
            f_out.write(f"    \\label{{fig:{f.split('.')[0]}}}\n")
            f_out.write("\\end{figure}\n\n")

    print(f"Success! {len(files)} LaTeX figures written to {output_file}")

if __name__ == "__main__":
    generate_latex_file()