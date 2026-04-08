import json
import psycopg2
import sys
import os
import time

# Helper function to easily grab a fresh connection
def get_connection(db, db_user, db_port):
    return psycopg2.connect(host="/tmp/", database=db, user=db_user, port=db_port)

def main(db, db_user, db_port):
    # Establish initial database connection
    try:
        conn = get_connection(db, db_user, db_port)
        cur = conn.cursor()
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        sys.exit(1)

    for part_number in range(14, 0, -1):
        part_suffix = 'part{:02d}'.format(part_number)
        json_file_path = f'/data/mettas/Join-Game/data/WDC/offers_corpus_all_v2.json.{part_suffix}'
        
        if not os.path.exists(json_file_path):
            print(f"File not found, skipping: {json_file_path}")
            continue
            
        print(f"Processing {json_file_path}...")
        
        with open(json_file_path, 'r') as file:
            a = 1
            for index, line in enumerate(file):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    item = json.loads(line)
                except ValueError as e:
                    continue
                
                try:
                    # 1. AUTO-RECONNECT: If the DB dropped us, establish a new connection
                    if conn.closed != 0:
                        print(f"Connection lost at line {index}. Reconnecting...")
                        time.sleep(2) # Brief pause before hammering the server
                        conn = get_connection(db, db_user, db_port)
                        cur = conn.cursor()

                    conn.rollback()  
                    cur.execute("BEGIN;")

                    # Extract values
                    id_ = item.get('id', None)
                    title = item.get('title', None)
                    description = item.get('description', None)
                    brand = item.get('brand', None)
                    price = item.get('price', None)
                    category = item.get('category', None)
                    cluster_id = item.get('cluster_id', None)
                    
                    if brand is not None:
                        cur.execute("""
                            INSERT INTO wdc1Brands (id, title, description, brand, price, category, cluster_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            id_, title, description, brand, price, category, cluster_id
                        ))
                        conn.commit()  

                    if a % 5000 == 0:
                        print(f"Inserted {a} valid branded records from {part_suffix}...")
                    a += 1

                except psycopg2.OperationalError as e:
                    # Happens if the connection drops MID-query
                    print(f"Operational error at line {index} (will retry connection on next loop): {e}")
                except psycopg2.IntegrityError as e:
                    # 2. SAFE ROLLBACK: Only roll back if the connection is still actually open
                    if conn.closed == 0:
                        conn.rollback() 
                except Exception as e:
                    print(f"Error processing item at line {index}: {e}")
                    if conn.closed == 0:
                        conn.rollback() 

        print(f"Finished processing {json_file_path}")

    cur.close()
    if conn.closed == 0:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 wdc_upload.py <database> <user> <port>")
        sys.exit(1)

    db = sys.argv[1]
    db_user = sys.argv[2]
    db_port = sys.argv[3]

    main(db, db_user, db_port)