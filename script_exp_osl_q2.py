#!/bin/python
import datetime
import psycopg2
from time import time
import sys

def main():
    ks = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 1050, 1100, 1150, 1200, 2000, 3000, 4000, 5000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000]
    sigma = .99
    # Open connection
    conn = psycopg2.connect(host="/tmp/", database="srikhakb", user="srikhakb", port="5447")
    cur = conn.cursor()

    if (len(sys.argv) != 3):
        print("Expecting 2 arguments, verbose output filename and summary filename")
        exit()

    # Open files for writing
    data_filename = str(sys.argv[1])
    summary_filename = str(sys.argv[2])
    summary = open(summary_filename, 'w+')

    # Build queries for testing
    joinQueries = constructQueries()

    # Data structure to hold times for averaging
    k_times = {k: {'unweighted': [], 'weighted': []} for k in ks}

    # Begin iterations
    loop = 1
    for joinQuery in joinQueries:
        timeForKs = []

        cur.execute('set statement_timeout = 0;')

        for i in range(loop):
            print("Running this query for the " + str(i) + " time(s)")
            timeForKs.append(measureTimeForKs(conn, joinQuery, ks, sigma, data_filename, i))
        summary.write("\tQuery: %s\n" % (joinQuery))
        minLenRun = sys.maxsize
        for i in range(len(timeForKs)):
            minLenRun = min(minLenRun, len(timeForKs[i]))

        for j in range(minLenRun):
            normalsum = 0
            weightedsum = 0
            for i in range(len(timeForKs)):
                normalsum += timeForKs[i][j][1]
                weightedsum += timeForKs[i][j][2]
                kCurr = timeForKs[i][j][0]
            summary.write("K val:%i\tAverage time (unweighted): %f\t Average time (weighted): %f\n" % (
            kCurr, normalsum / len(timeForKs), weightedsum / len(timeForKs)))
            # Update k_times for averaging
            k_times[kCurr]['unweighted'].append(normalsum / len(timeForKs))
            k_times[kCurr]['weighted'].append(weightedsum / len(timeForKs))
        summary.flush()
    # After processing all queries, write averages to summary in the new format
    summary.write("\nAverage Times Across All Queries:\n")
    for k in ks:
        avg_unweighted = sum(k_times[k]['unweighted']) / len(k_times[k]['unweighted']) if k_times[k]['unweighted'] else 0
        avg_weighted = sum(k_times[k]['weighted']) / len(k_times[k]['weighted']) if k_times[k]['weighted'] else 0
        # Updated format
        summary.write("(%i, %f) (%i, %f)\n" % (k, avg_unweighted, k, avg_weighted))

    print("Got here!")
    # Close files before exiting
    summary.close()
    exit()

def constructQueries():
    result = []
    val = '1_5'
    sch_vals = ['1', '3', '4']
    for sch_val in sch_vals:
        # Q2 - TPCH
        result.append("select * from part%s%s, supplier%s%s, partsupp%s%s where p_partkey = ps_partkey and s_suppkey = ps_suppkey;" % (sch_val, val, sch_val, val, sch_val, val))
    return result

# measures the running time of a join query for a given value of k
def measureTimeForKs(conn, joinQuery, ks, sigma, data_filename, iteration):
    f = None
    cur = None
    try:
        f = open(data_filename, 'a')
        cur = conn.cursor()

        # Database config options
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
        cur.execute('set work_mem = "64kB";')
        cur.execute('set statement_timeout = 1800000;') #1800000 = 30 mins

        f.write("======================================================== \n")
        f.write("Time of the test run: " + str(datetime.datetime.now()) + '\n')
        f.write("BNL: ")
        f.write(joinQuery + " #" + str(iteration + 1) + '\n')

        # Make the cursor server-side by naming it
        cur = conn.cursor('cur_uniq')

        cur.itersize = 1
        start = time()

        cur.execute(joinQuery)

        f.write('  time before fetch: %f sec' % (time() - start))
        fetched = int(0)
        start = time()
        prev = start
        factor = sigma
        weightedTime = 0
        res = []

        barrier = int(50)
        print("Query started: " + joinQuery)

        for _ in cur:
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
            if joinTime >= 900: #900 = 15 min, 1500 = 25 min
                break

        f.write("Total joined tuples fetched: %d,%f\n" % (fetched, joinTime))
        f.write('Time of current query run: %.2f sec\n' % (joinTime) + '\n')

    except psycopg2.errors.QueryCanceledError as e:
        print("Query was canceled due to timeout: %s" % e)
    except Exception as e:
        print("An unexpected error occurred: %s" % e)
    finally:
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
