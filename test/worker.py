#!/bin/python
import sys
import datetime
import psycopg2
import csv
from time import time
import os

# Database connection details
USER = 'jinjo'
HOST = '/tmp/'
PORT = '1531'

# Parameters setting for the join algorithm
SIGMA = 0.99
DATA_LIMIT = 100
STEP_SIZE = 1.5
ITER_SIZE = 100
TEST_NUMBER = 1

data_points = [int(ITER_SIZE * (STEP_SIZE ** i)) for i in range(100)]

def get_mj_total(db_name, sql):
    try:
        conn = psycopg2.connect(dbname=db_name, user=USER, host=HOST, port=PORT)
        conn.autocommit = False 
        with conn.cursor() as setup_cur:
            setup_cur.execute("SET work_mem = '64kB'; SET statement_timeout = 3600000;")
            setup_cur.execute("SET enable_hashjoin=off; SET enable_mergejoin=on; SET enable_nestloop=off;")
        conn.commit()
        with conn.cursor(name='mj_cur') as mj_cur:
            mj_cur.itersize = 2000
            mj_cur.execute(sql)
            total = sum(1 for _ in mj_cur)
        conn.close()
        return total
    except Exception as e:
        return -1

def join_query(conn, server_cur, csv_writer, log_file, total_tuples, time_limit):
    current_phase, start_time, fetched_count, weighted_time = 1, time(), 0, 0
    prev_time, factor, idx = start_time, SIGMA, 0
    target_tuples = int(total_tuples * 0.10) if total_tuples > 0 else float('inf')
    
    for _ in server_cur:
        while conn.notices:
            notice = conn.notices.pop(0)
            if "Phase 2" in notice: current_phase = 2
            elif "Phase 3" in notice: current_phase = 3

        fetched_count += 1
        current_time = time()
        weighted_time += (current_time - prev_time) * factor
        prev_time, factor = current_time, factor * SIGMA
        
        if fetched_count % ITER_SIZE == 0:
            cumulative_time = current_time - start_time
            if fetched_count >= data_points[idx]:
                pct_output = (fetched_count / total_tuples) * 100 if total_tuples > 0 else 0.0
                csv_writer.writerow([fetched_count, round(cumulative_time, 4), current_phase, round(pct_output, 4)])
                idx += 1
            
            if cumulative_time >= time_limit or idx >= DATA_LIMIT: 
                if cumulative_time >= time_limit:
                    log_file.write(f"Timeout: {time_limit} sec reached.\n")
                break
        if fetched_count >= target_tuples: break

    csv_writer.writerow([fetched_count, round(time() - start_time, 4), current_phase, 10.0])

# Modified to take db_name directly as an argument
def run_worker(db_name, q_name, val, mem, time_limit, sch_val, sql):
    time_limit = int(time_limit)
    os.makedirs('results', exist_ok=True)
    
    total_tuples = get_mj_total(db_name, sql)
    if total_tuples < 0: return

    # Using db_name in the file prefix for organization
    file_prefix = f"results/{q_name}_{db_name}_z{val}_{mem.lower()}"
    
    with open(f"{file_prefix}.log", 'w') as log_file, open(f"{file_prefix}_sch{sch_val}.csv", 'w', newline='') as f_csv:
        csv_writer = csv.writer(f_csv)
        csv_writer.writerow(['k_tuples', 'time_sec', 'phase', 'pct_output'])
        
        conn = psycopg2.connect(dbname=db_name, user=USER, host=HOST, port=PORT)
        with conn.cursor() as setup_cur:
            setup_cur.execute(f"SET work_mem = '{mem}'; SET statement_timeout = {time_limit * 1000}; SET enable_hashjoin = ON; SET enable_mergejoin = OFF;")
        conn.commit()
        
        log_file.write(f"========================================================\n")
        log_file.write(f"Time: {datetime.datetime.now()} | Database: {db_name} | Query: {q_name}\n")
        log_file.write(f"Executing: {sql}\n")
        
        with conn.cursor(name='ehj_cursor') as sc:
            sc.itersize = ITER_SIZE
            sc.execute(sql)
            join_query(conn, sc, csv_writer, log_file, total_tuples, time_limit)
        conn.close()

        # Notification content updated to be database-agnostic
        subject = f"Run Complete: {q_name} | DB: {db_name} | Z:{val} | {mem}"
        body = f"The worker has finished processing {q_name} on {db_name} with Z={val} and {mem} memory."
        os.system(f'echo "{body}" | mail -s "{subject}" jinjo@oregonstate.edu')

if __name__ == "__main__":
    # Updated to expect 7 arguments + script name = 8 total items
    if len(sys.argv) == 8:
        run_worker(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7])
    else:
        print("Usage: python3 worker.py <db_name> <q_name> <z_val> <mem> <time_limit_in_seconds> <sch_val> <sql_string>")