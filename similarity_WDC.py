#!/bin/python
import datetime
import psycopg2
from time import time
import sys
import os

def main():
    algorithm = sys.argv[3]  # Capture the algorithm name from command line arguments

    ks = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 600, 700, 800, 900,
          1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000,
          14000, 15000, 17500, 20000, 22500, 25000, 30000, 35000, 40000, 45000, 50000, 60000,
          70000, 80000, 90000, 100000, 110000, 120000, 130000, 140000, 150000, 160000, 170000,
          180000, 190000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000,
          1100000, 1200000, 1300000, 1400000, 1500000, 1600000, 1700000, 1800000, 1900000, 2000000,
          3000000, 4000000, 5000000, 6000000]
    sigma = 0.99
    vals = ['1', '2', '3']
    lv = ['1', '2', '3']

    conn = psycopg2.connect(host="/tmp/", database="mettas", user="mettas", port="1997")
    cur = conn.cursor()

    if len(sys.argv) != 4:
        print("Expecting 3 arguments: verbose output filename, summary filename, and algorithm name")
        sys.exit()

    data_filename = sys.argv[1]
    summary_filename = sys.argv[2]

    joinQueries = constructQueries(vals, lv)

    k_times = {(k, val, dist): {'unweighted': [], 'weighted': []} for k in ks for val in vals for dist in lv}
    k_times.update({('others', val, dist): {'unweighted': [], 'weighted': []} for val in vals for dist in lv})

    loop = 1
    for joinQuery, val, dist in joinQueries:
        timeForKs = []

        # if sys.argv[4] != None:
        #     full_path = "/data/mettas/Join-Game/Join_Full/"+sys.argv[4]+"/"+algorithm
        #     if not os.path.exists(full_path):
        #         os.makedirs(full_path)
        # else:
        full_path = "/data/mettas/Join-Game/Join_Full/"+algorithm
        
        with open(os.path.join(full_path, f"{data_filename}_lv{dist}_WDC{val}"), 'w+') as data:
            cur.execute('SET statement_timeout = 0;')  # Setting statement timeout to zero for no limit
            
            for i in range(loop):
                print(f"Running this query for the {i} time(s)")
                timeForKs.append(measureTimeForKs(conn, joinQuery, ks, sigma, f"{data_filename}_WDC", i))

            data.write(f"\tQuery: {joinQuery[0]}\n")
            
            minLenRun = min(len(run) for run in timeForKs)
            for j in range(minLenRun):
                normalsum = sum(run[j][1] for run in timeForKs)
                weightedsum = sum(run[j][2] for run in timeForKs)
                kCurr = timeForKs[0][j][0]

                target_key = (kCurr, val, dist) if kCurr in ks else ('others', val, dist)
                data.write(f"K val:{kCurr}\tAverage time (unweighted): {normalsum / len(timeForKs)}\t Average time (weighted): {weightedsum / len(timeForKs)}\n")
                k_times[target_key]['unweighted'].append(normalsum / len(timeForKs))
                k_times[target_key]['weighted'].append(weightedsum / len(timeForKs))

    write_summaries(vals, lv, ks, k_times, summary_filename, algorithm)

def write_summaries(vals, lv, ks, k_times, summary_filename, algorithm):
    for val in vals:
        for dist in lv:

            # if sys.argv[4] != None:
            #     unweighted_path = "/data/mettas/Join-Game/Join_Summary/"+sys.argv[4]+"/"+algorithm+"/unweighted/"
            #     weighted_path = "/data/mettas/Join-Game/Join_Summary/"+sys.argv[4]+"/"+algorithm+"/weighted/"
            #     if not os.path.exists(unweighted_path):
            #         os.makedirs(unweighted_path)
            #     if not os.path.exists(weighted_path):
            #         os.makedirs(weighted_path)
            # else:
            unweighted_path = "/data/mettas/Join-Game/Join_Summary/"+algorithm+"/unweighted/"
            weighted_path = "/data/mettas/Join-Game/Join_Summary/"+algorithm+"/weighted/"

            with open(os.path.join(unweighted_path, f"{summary_filename}_lv{dist}_WDC{val}_unweighted.dat"), 'w+') as summary_unweighted, \
                 open(os.path.join(weighted_path, f"{summary_filename}_lv{dist}_WDC{val}_weighted.dat"), 'w+') as summary_weighted:
                for k in ks:
                    unweighted_avg = sum(k_times[(k, val, dist)]['unweighted']) / len(k_times[(k, val, dist)]['unweighted']) if k_times[(k, val, dist)]['unweighted'] else 0
                    weighted_avg = sum(k_times[(k, val, dist)]['weighted']) / len(k_times[(k, val, dist)]['weighted']) if k_times[(k, val, dist)]['weighted'] else 0
                    
                    summary_unweighted.write(f"{k} \t{unweighted_avg}\n")
                    summary_weighted.write(f"{k} \t{weighted_avg}\n")

    print("Summary files written successfully.")

def constructQueries(vals, lv):
    return  [("""SELECT wdc1Brands{0}.brand, wdc2Brands{0}.brand, wdc3Brands{0}.brand 
                FROM wdc1Brands{0} 
                JOIN wdc2Brands{0} 
                ON levenshtein(trim(wdc1Brands{0}.brand::varchar(10)), trim(wdc2Brands{0}.brand::varchar(10))) <= {1} 
                JOIN wdc3Brands{0} 
                ON levenshtein(trim(wdc1Brands{0}.brand::varchar(10)), trim(wdc3Brands{0}.brand::varchar(10))) <= {1} 
                AND levenshtein(trim(wdc2Brands{0}.brand::varchar(10)), trim(wdc3Brands{0}.brand::varchar(10))) <= {1} 
                LIMIT 3007;""".format(val, dist)), val, dist) for val in vals for dist in lv]

def measureTimeForKs(conn, joinQuery, ks, sigma, data_filename, iteration):
    results = []
    with open(data_filename, 'a') as f, conn.cursor() as cur:
        set_session_settings(cur)
        f.write("======================================================== \n")
        f.write(f"Time of the test run: {datetime.datetime.now()}\n")
        f.write(f"BNL: {joinQuery} #{iteration + 1}\n")

        cur.execute(joinQuery)
        fetched = 0
        start = time()
        prev = start
        weightedTime = 0
        factor = sigma
        barrier = 50

        for row in cur:
            fetched += 1
            current = time()
            weightedTime += (current - prev) * factor
            prev = current
            factor *= sigma
            joinTime = current - start
            if fetched == barrier:
                barrier += 50
                f.write(f"{fetched}, {joinTime:.2f}, {weightedTime:.2f}\n")
            if fetched in ks:
                results.append([fetched, joinTime, weightedTime])
            if joinTime >= 1800:  # Stop after 30 mins to avoid excessively long queries
                break

        if fetched == 0:
            print("No rows fetched. Check if the tables have data.")

        if fetched not in ks:
            results.append([fetched, joinTime, weightedTime])
            f.write(f"Final fetch count before exit: {fetched}, {joinTime:.2f}, {weightedTime:.2f}\n")
        f.write(f"Total joined tuples fetched: {fetched}, {joinTime:.2f}\n")

    return results

def set_session_settings(cur):
    settings = [
        'enable_material=off', 'max_parallel_workers_per_gather=0', 'enable_hashjoin=off',
        'enable_mergejoin=off', 'enable_indexonlyscan=off', 'enable_indexscan=off',
        'enable_block=off', 'enable_bitmapscan=off', 'enable_fastjoin=off', 'enable_seqscan=off',
        'enable_fliporder=off', 'enable_nestloop=on', 'work_mem=10MB', 'statement_timeout=1800000'  # 30 minutes
    ]
    for setting in settings:
        cur.execute(f'SET {setting};')

if __name__ == '__main__':
    main()
