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
          3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000]
    sigma = 0.99
    vals = ['1', '2', '3']
    lv = ['1', '2', '3']

    conn = psycopg2.connect(host="/tmp/", database="mettas", user="mettas", port="1506")
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
        #     full_path = "/data/saketh_temp/PSQL1506/Join-Game/Join_Full/"+sys.argv[4]+"/"+algorithm
        #     if not os.path.exists(full_path):
        #         os.makedirs(full_path)
        # else:
        full_path = "/data/saketh_temp/PSQL1506/Join-Game/Join_Full/"+algorithm
        
        with open(os.path.join(full_path, f"{data_filename}_lv{dist}_WDC{val}"), 'w+') as data:
            cur.execute('SET statement_timeout = 18000;')  # Setting statement timeout to zero for no limit
            
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
            #     unweighted_path = "/data/saketh_temp/PSQL1506/Join-Game/Join_Summary/"+sys.argv[4]+"/"+algorithm+"/unweighted/"
            #     weighted_path = "/data/saketh_temp/PSQL1506/Join-Game/Join_Summary/"+sys.argv[4]+"/"+algorithm+"/weighted/"
            #     if not os.path.exists(unweighted_path):
            #         os.makedirs(unweighted_path)
            #     if not os.path.exists(weighted_path):
            #         os.makedirs(weighted_path)
            # else:
            unweighted_path = "/data/saketh_temp/PSQL1506/Join-Game/Join_Summary/"+algorithm+"/unweighted/"
            weighted_path = "/data/saketh_temp/PSQL1506/Join-Game/Join_Summary/"+algorithm+"/weighted/"

            with open(os.path.join(unweighted_path, f"{summary_filename}_lv{dist}_WDC{val}_unweighted.dat"), 'w+') as summary_unweighted, \
                 open(os.path.join(weighted_path, f"{summary_filename}_lv{dist}_WDC{val}_weighted.dat"), 'w+') as summary_weighted:
                for k in ks:
                    unweighted_avg = sum(k_times[(k, val, dist)]['unweighted']) / len(k_times[(k, val, dist)]['unweighted']) if k_times[(k, val, dist)]['unweighted'] else 0
                    weighted_avg = sum(k_times[(k, val, dist)]['weighted']) / len(k_times[(k, val, dist)]['weighted']) if k_times[(k, val, dist)]['weighted'] else 0
                    
                    summary_unweighted.write(f"{k} \t{unweighted_avg}\n")
                    summary_weighted.write(f"{k} \t{weighted_avg}\n")

    print("Summary files written successfully.")

def constructQueries(vals, lv):
    # limits = ['1328257', '2001040', '6433621']
    limits = ['8042489', '9133633', '3809461']
    result = []
    for val in vals:
        for dist in lv:
            # query = ("""SELECT wdc1Brands{0}.brand, wdc2Brands{0}.brand 
            #         FROM wdc1Brands{0} 
            #         JOIN wdc2Brands{0} 
            #         ON levenshtein(trim(wdc1Brands{0}.brand::varchar(10)), trim(wdc2Brands{0}.brand::varchar(10))) <= {1} 
            #         LIMIT {2};""".format(val, dist, limits[int(dist)-1]))
            query = ("""SELECT sub.brand1, sub.brand2, wdc3brands{0}.brand
                        FROM (
                            SELECT wdc1brands{0}.brand AS brand1, wdc2brands{0}.brand AS brand2
                            FROM wdc1brands{0}
                            JOIN wdc2brands{0} ON levenshtein(trim(wdc1brands{0}.brand::varchar(10)), trim(wdc2brands{0}.brand::varchar(10))) <= {1}
                        ) sub
                        JOIN wdc3brands{0}
                            ON levenshtein(trim(sub.brand1::varchar(10)), trim(wdc3brands{0}.brand::varchar(10))) <= {1}
                            AND levenshtein(trim(sub.brand2::varchar(10)), trim(wdc3brands{0}.brand::varchar(10))) <= {1}
                        LIMIT {2};""".format(val, dist, limits[int(dist)-1]))
            result.append((query, val, dist))
    return result

def measureTimeForKs(conn, joinQuery, ks, sigma, data_filename, iteration):
    f = None
    cur = None
    res = []  # Initialize res to ensure it exists
    try:
        # Open the specified data file in append mode
        f = open(data_filename, 'a')
        cur = conn.cursor()

        # Set database session settings to control query execution environment
        cur.execute('set enable_material=off;')
        cur.execute('set max_parallel_workers_per_gather=0;')
        cur.execute('set enable_hashjoin=off;')
        cur.execute('set enable_mergejoin=off;')
        cur.execute('set enable_indexonlyscan=off;')
        cur.execute('set enable_indexscan=off;')
        cur.execute('set enable_block=off;')
        cur.execute('set enable_bitmapscan=off;')
        cur.execute('set enable_fastjoin=off;')
        cur.execute('set enable_seqscan=off;')
        cur.execute('set enable_fliporder=off;')
        cur.execute('set enable_nestloop=on;')
        cur.execute('set work_mem = "512MB";')
        cur.execute('set statement_timeout = 1800000;') #1800000 = 30 mins
        
        # Log start of test run
        f.write("======================================================== \n")
        f.write("Time of the test run: " + str(datetime.datetime.now()) + '\n')
        f.write("BNL: ")
        f.write(joinQuery + " #" + str(iteration + 1) + '\n')

        # Execute the join query
        cur = conn.cursor('cur_uniq')

        cur.itersize = 2000
        start = time()

        cur.execute(joinQuery)
        
        # Measure and log time before fetch
        f.write('  time before fetch: %f sec\n' % (time() - start))
        fetched = int(0)
        start = time()
        prev = start
        factor = sigma
        weightedTime = 0
        
        # Fetch results and measure weighted time
        barrier = int(50)
        print("Query started: " + joinQuery)

        for row in cur:
            fetched += 1
            current = time()
            weightedTime += (current - prev) * factor
            prev = current
            factor *= sigma
            joinTime = current - start
            if fetched == barrier:
                barrier += 50
                f.write("%d, %f, %f\n" % (fetched, joinTime, weightedTime))
            if fetched in ks:
                res.append([fetched, joinTime, weightedTime])
            if joinTime >= 1800: #900 = 15 min, 1500 = 25 min
                break

        if fetched == 0:
            print("No rows fetched. Check if the tables have data.")
                
        # Additional logging for last fetched count if not in ks
        if fetched not in ks:
            res.append([fetched, joinTime, weightedTime])
            f.write("Final fetch count before exit: %d, %f, %f\n" % (fetched, joinTime, weightedTime))
            
        # Log total joined tuples and current query run time
        f.write("Total joined tuples fetched: %d, %f\n" % (fetched, joinTime))
        f.write('Time of current query run: %.2f sec\n' % (joinTime) + '\n')

    except psycopg2.errors.QueryCanceledError as e:
        print("Query was canceled due to timeout: %s" % e)
    except psycopg2.InterfaceError as e:
        print("Database connection issue: %s" % e)
    except Exception as e:
        print("An unexpected error occurred: %s" % e)
    finally:
        # Ensure resources are closed
        if cur is not None:
            cur.close()
        if f is not None:
            f.flush()
            f.close()

    if not res:
        res.append([0, 0, 0])
    return res

if __name__ == '__main__':
    main()
