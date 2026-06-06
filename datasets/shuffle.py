#!/bin/python
import datetime
import psycopg2
import sys

def main():
    zvals = ['0', '1', '1_5']
    shuffles = ['1', '3', '2']
    
    # Connection details for your environment
    conn = psycopg2.connect(host="localhost", database="ehj", user="jinjo", port="1531")
    
    # Create the new shuffled schemas and tables
    createShuffledTables(shuffles, zvals, conn)
    
    conn.close()
    print("All tasks completed.")
    exit()

def makeRandomCopyString(base_schema, new_schema, table):
    """Generates the SQL to copy and shuffle data directly within the database."""
    print(f"Time of shuffle start: {datetime.datetime.now()}")
    
    return f"CREATE TABLE {new_schema}.{table} AS SELECT * FROM {base_schema}.{table} ORDER BY random();"

def createShuffledTables(shuffles, zvals, conn):
    """Creates the shuffled schemas (e.g., z0_shuff_1) and populates them randomly."""
    cur = conn.cursor()
    
    # Updated to use 'orders' to match your database schema
    tables = ["part", "orders", "lineitem", "supplier", "customer", "partsupp"]

    for scf in shuffles:
        for val in zvals:
            # Fixed schema format: z0, z1, z1_5
            base_schema = f"z{val}"
            new_schema = f"z{val}_shuff{scf}"
            
            print(f"\n--- Recreating shuffle schema: {new_schema} ---")
            
            # Create the target schema if it doesn't exist
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {new_schema};")
            
            for t in tables:
                # Drop existing tables inside the specific schema
                cur.execute(f"DROP TABLE IF EXISTS {new_schema}.{t};")
                print(f"Dropped {new_schema}.{t}")
                
                # Copy from the base schema into the shuffled schema with ORDER BY random()
                cur.execute(makeRandomCopyString(base_schema, new_schema, t))
                print(f"Populated {new_schema}.{t} randomly")

            conn.commit()

if __name__ == '__main__':
    main()