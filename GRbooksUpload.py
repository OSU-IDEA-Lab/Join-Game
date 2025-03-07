from __future__ import print_function
import json
import psycopg2
from datetime import datetime

# Establish database connection
conn = psycopg2.connect(host="/tmp/", database="mettas", user="mettas", port="1997")
cur = conn.cursor()

def insert_book_data(data):
    # Convert string date to date object, handling cases where the date might be missing or malformed
    try:
        pub_date = datetime.strptime(data['original_publication_date'], '%Y-%m-%d').date() if 'original_publication_date' in data and data['original_publication_date'] else None
    except ValueError:
        pub_date = None  # Default to None if there's an error in date conversion
    
    # Check if the id already exists in the database
    id_ = data.get('id')
    if id_ is not None:
        cur.execute("SELECT id FROM gr_books WHERE id = %s", (id_,))
        if cur.fetchone():
            print("Skipping existing book with id: {}".format(id_))
            return  # Skip this function if id exists

    # If id does not exist, insert the new book data
    cur.execute('''
        INSERT INTO gr_books (id, title, author_name, language, original_publication_date, description)
        VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            id_,
            data.get('title'),
            data.get('author_name'),
            data.get('language'),
            pub_date,  # Use the converted date
            data.get('description'),
        ))

a = 0

# Process multiple JSON files
for i in range(1, 27):
    json_file_path = '/data/mettas/data/Books/books.json.part{0:02}'.format(i)
    
    with open(json_file_path, 'r') as file:
        for line in file:
            try:
                books = json.loads(line.strip())
                conn.rollback()  # Rollback to clear any previous errors
                cur.execute("BEGIN;")
                insert_book_data(books)
                conn.commit()  # Commit after each successful insert
                print(a)
                a += 1
            except ValueError as e:
                conn.rollback()  # Ensure the transaction is clean for the next iteration
                print("Failed to decode JSON in file {}: {}".format(json_file_path, e))

# Close the database connection
cur.close()
conn.close()
