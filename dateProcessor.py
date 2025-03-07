import csv
import re

input_file = '/data/mettas/data/Book_review/books_data.csv'
output_file = '/data/mettas/data/Book_review/formatted_books_data.csv'

def is_valid_date(date_str):
    # Check if the date is in valid YYYY, YYYY-MM, or YYYY-MM-DD format
    return bool(re.match(r'^\d{4}$|^\d{4}-\d{2}$|^\d{4}-\d{2}-\d{2}$', date_str))

with open(input_file, 'r', encoding='utf-8') as csvfile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(csvfile)
    fieldnames = reader.fieldnames
    
    # Ensure 'publishedDate' is in the fieldnames
    if 'publishedDate' not in fieldnames:
        raise KeyError("The column 'published_date' does not exist in the CSV file.")
    
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in reader:
        if 'publishedDate' in row and row['publishedDate'].strip():
            if len(row['publishedDate']) == 4:  # Format: YYYY
                row['publishedDate'] = row['publishedDate'] + '-01-01'
            elif len(row['publishedDate']) == 7:  # Format: YYYY-MM
                row['publishedDate'] = row['publishedDate'] + '-01'
            elif not is_valid_date(row['publishedDate']):  # Invalid format
                row['publishedDate'] = None
        
        writer.writerow(row)
