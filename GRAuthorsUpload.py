import json
import psycopg2
from datetime import date

conn = psycopg2.connect(host="/tmp/", database="mettas", user="mettas", port="1997")
cur = conn.cursor()

# Create table
cur.execute('''
CREATE TABLE IF NOT EXISTS gr_authors (
    id TEXT PRIMARY KEY,
    name TEXT,
    gender TEXT,
    ratings_count INTEGER,
    average_rating FLOAT,
    text_reviews_count INTEGER,
    works_count INTEGER,
    fans_count INTEGER
);
''')

# Function to insert data into Authors table
def insert_author_data(data):
    cur.execute('''
    INSERT INTO gr_authors (id, name, gender, ratings_count, average_rating, text_reviews_count, works_count, fans_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        data['id'],
        data['name'],
        data['gender'],
        int(data['ratings_count']),
        float(data['average_rating']),
        int(data['text_reviews_count']),
        int(data['works_count']),
        int(data['fans_count'])
    ))

a = 0

# Load data from JSON file
json_file_path = '/data/mettas/data/authors.json'
with open(json_file_path, 'r') as file:
    for line in file:
        try:
            author = json.loads(line.strip())
            insert_author_data(author)
            conn.commit()
        except json.JSONDecodeError as e:
            print("Failed to decode JSON:", e)
        
        print(a)
        a+=1

# Commit and close
conn.commit()
cur.close()
conn.close()