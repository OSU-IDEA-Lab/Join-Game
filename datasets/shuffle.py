#!/usr/bin/env python3
import datetime
import psycopg2

def main():
    # Define the skew factors (z-values) and shuffle iterations to generate
    skew_factors = ['0', '1', '1_5']
    shuffle_ids = ['1', '2', '3'] 
    
    # Connect directly to the 10GB TPC-H database
    conn = psycopg2.connect(
        host="localhost", 
        database="tpch10g", 
        user="jinjo", 
        port="1531"
    )
    
    print("Starting database shuffle process...")
    
    # Execute the shuffling process
    create_shuffled_tables(shuffle_ids, skew_factors, conn)
    
    # Clean up and close connection
    conn.close()
    print("\nAll shuffling tasks completed successfully.")

def generate_shuffle_sql(base_schema, target_schema, table_name):
    """
    Generates the SQL command to duplicate and randomize a table.
    Uses 'ORDER BY random()' to physically shuffle the rows as they are inserted.
    """
    current_time = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"  [{current_time}] Shuffling data into {target_schema}.{table_name}...")
    
    return f"CREATE TABLE {target_schema}.{table_name} AS SELECT * FROM {base_schema}.{table_name} ORDER BY random();"

def create_shuffled_tables(shuffle_ids, skew_factors, conn):
    """
    Iterates through all combinations of skew factors and shuffle IDs,
    creating isolated schemas (e.g., z0_shuff1) and populating them with randomized data.
    """
    cur = conn.cursor()
    
    # The core TPC-H tables required for the benchmarks
    tables = ["part", "orders", "lineitem", "supplier", "customer", "partsupp"]

    for shuffle_id in shuffle_ids:
        for skew in skew_factors:
            
            # Define source and destination schemas
            base_schema = f"z{skew}"                      # Example: z0
            target_schema = f"z{skew}_shuff{shuffle_id}"  # Example: z0_shuff1
            
            print(f"\n{'='*45}")
            print(f" Preparing Target Schema: {target_schema}")
            print(f"{'='*45}")
            
            # Ensure the destination schema exists before writing to it
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")
            
            for table_name in tables:
                # Clear out any old data if the table already exists in the target schema
                cur.execute(f"DROP TABLE IF EXISTS {target_schema}.{table_name};")
                
                # Generate and execute the randomization SQL query
                shuffle_query = generate_shuffle_sql(base_schema, target_schema, table_name)
                cur.execute(shuffle_query)
                
            # Commit the changes for this specific schema combination
            conn.commit()

if __name__ == '__main__':
    main()