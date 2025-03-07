# Assuming the files contain numerical values on the first line which can be averaged
# datasets = ['ABR', 'GRBR', 'WDC', 'Spotify', 'Cars', 'Movies']
import sys
import os

def main():
    algorithm = sys.argv[1]  # Read algorithm from command line argument

    datasets = ['Cars']
    lvs = ['1', '2', '3']
    movie_lvs = ['9', '10', '11']
    weights = ['weighted', 'unweighted']

    for sets in datasets:
        if sets != 'movies':
            lvs_to_use = movie_lvs if sets == 'movies' else lvs
            for lv in lvs_to_use:
                for weight in weights:
                    file_paths = [
                        f'/data/mettas/Join-Game/Join_Summary/{algorithm}/{weight}/sum_similarity_lv{lv}_{sets}1_{weight}.dat',
                        f'/data/mettas/Join-Game/Join_Summary/{algorithm}/{weight}/sum_similarity_lv{lv}_{sets}2_{weight}.dat',
                        f'/data/mettas/Join-Game/Join_Summary/{algorithm}/{weight}/sum_similarity_lv{lv}_{sets}3_{weight}.dat'
                    ]

                    output_file_path = f'/data/mettas/Join-Game/Results/{algorithm}/sum_similarity_lv{lv}_{sets}_avg_{weight}.dat'

                    # Process files and calculate averages
                    try:
                        with open(file_paths[0], 'r') as file1, open(file_paths[1], 'r') as file2, open(file_paths[2], 'r') as file3, open(output_file_path, 'w') as output_file:
                            for line1, line2, line3 in zip(file1, file2, file3):
                                id1, value1 = line1.split('\t')
                                _, value2 = line2.split('\t')
                                _, value3 = line3.split('\t')

                                avg_value = (float(value1) + float(value2) + float(value3)) / 3
                                output_file.write(f"{id1}\t{avg_value:.10f}\n")

                        print(f"Processed and written averages to {output_file_path}")
                    except Exception as e:
                        print(f"Error processing files for {sets}, level {lv}, {weight}: {str(e)}")

if __name__ == "__main__":
    main()