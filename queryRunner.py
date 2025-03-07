import psycopg2

def connect_db():
    return psycopg2.connect(host="/tmp/", database="mettas", user="mettas", port="1997")

def query_running():
    query = """ explain analyse select * from partsupp1011_5, lineitem1011_5 where ps_partkey = l_partkey LIMIT 2400000;"""

    # query = """
    # EXPLAIN ANALYSE 
    # SELECT car_brands1.make, parking_tickets1.vehicle_make 
    # FROM car_brands1 
    # JOIN parking_tickets1 
    # ON levenshtein(trim(car_brands1.make::varchar(10)), trim(parking_tickets1.vehicle_make::varchar(10))) <= 1 
    # LIMIT 797181;"""
    
    # """
    # EXPLAIN ANALYSE
    # SELECT wdc1Brands.brand, wdc2Brands.brand
    # FROM wdc1Brands
    # JOIN wdc2Brands 
    # ON levenshtein(trim(wdc1Brands.brand::varchar(10)), trim(wdc2Brands.brand::varchar(10))) <= 1;"""

    settings = [
    'set max_parallel_workers_per_gather = 0;',
    'set enable_seqscan = ON;',
    'set enable_nestloop = ON;',
    'set enable_hashjoin = OFF;',
    'set enable_mergejoin = OFF;',
    'set work_mem = "5GB";',                  # Try to see difference varying work_mem
    'set statement_timeout = 180000000;',        # 30 mins = 1800000
    ]
    
    with connect_db() as conn:
        with conn.cursor() as cur:
            for setting in settings:
                cur.execute(setting)
            print("Query started: ", query)
            cur.execute(query)
            result = cur.fetchall()
            for row in result:
                print(row)  # Print the execution plan results

if __name__ == "__main__":
    query_running()
