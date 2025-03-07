import csv
import io

def preprocess_csv(input_path, output_path):
    with io.open(input_path, 'r', encoding='utf-8') as infile, \
         io.open(output_path, 'w', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile, quoting=csv.QUOTE_ALL, escapechar='"', doublequote=True)

        for row in reader:
            # Ensure all fields are treated as strings and quoted
            writer.writerow([field for field in row])

def preprocess_all_tables(base_input_path, base_output_path, table_names):
    for table_name in table_names:
        input_csv = '{}/{}.csv'.format(base_input_path, table_name)
        output_csv = '{}/{}.csv'.format(base_output_path, table_name)
        preprocess_csv(input_csv, output_csv)
        print("Preprocessed:", output_csv)

# List of table names
table_names = [
    "aka_name", "aka_title", "cast_info", "char_name", 
    "company_name", "movie_companies", "name", "title", 
    "movie_info", "person_info"
]

# Paths
base_input_path = '/data/mettas/data/IMDB/'
base_output_path = '/data/mettas/data/IMDB/'

# Preprocess all CSV files for the listed tables
preprocess_all_tables(base_input_path, base_output_path, table_names)
