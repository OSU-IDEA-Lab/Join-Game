import json
import psycopg2

# Establish database connection
conn = psycopg2.connect(host="/tmp/", database="mettas", user="mettas", port="1997")
cur = conn.cursor()

# Loop through JSON files with different part numbers
for part_number in range(7, 10):  # From part01 to part07
    # Format the part number with leading zeros
    part_suffix = 'part{:02d}'.format(part_number)
    json_file_path = '/data/mettas/data/WDC/offers_corpus_all_v2.json.{}'.format(part_suffix)
    
    # Load data from the JSON file
    with open(json_file_path, 'r') as file:
        content = file.read()
        data = []
        for obj in content.split('\n'):
            obj = obj.strip()
            if obj:
                try:
                    data.append(json.loads(obj))
                except ValueError as e:
                    print("Skipping invalid JSON object: {}".format(e))
                    continue
    
    a = 1
    for index, item in enumerate(data):
        try:
            # Begin a new transaction for each item
            conn.rollback()  # Ensure any previous error does not affect the current transaction
            cur.execute("BEGIN;")

            # Extracting values from the JSON structure with safe fallback
            id_ = item.get('id', None)
            title = item.get('title', None)
            description = item.get('description', None)
            brand = item.get('brand', None)
            price = item.get('price', None)
            category = item.get('category', None)
            cluster_id = item.get('cluster_id', None)
            
            # Insert into the wdc1 table with the additional columns
            if (brand != None):
                cur.execute("""
                    INSERT INTO wdc3Brands (id, title, description, brand, price, category, cluster_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_, title, description, brand, price, category,
                    cluster_id
                ))

                conn.commit()  # Commit the transaction after each successful insert
            print(a)
            a += 1
        except psycopg2.IntegrityError as e:
            conn.rollback()  # Rollback the transaction in case of an integrity error
            print("Skipping item due to IntegrityError: {}".format(e))
        except Exception as e:
            conn.rollback()  # Rollback the transaction in case of any other error
            print("Error processing item {}: {}".format(index, e))

    print("Finished processing {}".format(json_file_path))

# Close the database connection
cur.close()
conn.close()
