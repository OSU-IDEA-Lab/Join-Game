#!/bin/python
import datetime
import psycopg2
from time import time
import sys

def main():
    # Define the range of k values and the sigma for weighted timing calculations
    #ks = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 1050, 1100, 1150, 1200, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 12500, 15000, 17500, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 60000, 70000, 80000, 90000, 100000]
    ks = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000, 17500, 20000, 22500, 25000, 30000, 35000, 40000, 45000, 50000, 60000, 70000, 80000, 90000, 100000]
    sigma = .99
    vals = ['0', '1', '1_5']
    # Establish database connection
    conn = psycopg2.connect(host="/tmp/", database="xxxxxx", user="xxxxxx", port="xxxx")
    cur = conn.cursor()

    # Expecting two command-line arguments for output filenames
    if (len(sys.argv) != 3):
        print("Expecting 2 arguments, verbose output filename and summary filename")
        exit()

    # Open files for writing
    data_filename = str(sys.argv[1])
    summary_filename = str(sys.argv[2])
    summary = open(summary_filename, 'w+')

    # Generate SQL join queries for testing
    joinQueries = constructQueries(vals)

    # Initialize data structure for storing timing data
    k_times = {(k, val): {'unweighted': [], 'weighted': []} for k in ks for val in vals}
    k_times.update({('others', val): {'unweighted': [], 'weighted': []} for val in vals})  # Adding this to handle unexpected k-values


    # Execute each generated query and measure performance
    loop = 1
    for joinQuery, val, sch_val in joinQueries:
        timeForKs = []
        
        # Disable the statement timeout for long-running queries
        cur.execute('set statement_timeout = 0;')
        
        # Perform the query measurement loop times (currently 1)
        for i in range(loop):
            print("Running this query for the " + str(i) + " time(s)")
            timeForKs.append(measureTimeForKs(conn, joinQuery, ks, sigma, data_filename, i))
            
        # Write query information to summary
        summary.write("\tQuery: %s\n" % (joinQuery))
        
        # Process and average the timing data
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

            # Check if kCurr is a standard k value
            if kCurr in ks:
                target_key = (kCurr, val)
            else:
                target_key = ('others', val)
        
            summary.write("K val:%i\tAverage time (unweighted): %f\t Average time (weighted): %f\n" % (
            kCurr, normalsum / len(timeForKs), weightedsum / len(timeForKs)))
            k_times[target_key]['unweighted'].append(normalsum / len(timeForKs))
            k_times[target_key]['weighted'].append(weightedsum / len(timeForKs))
        summary.flush()
        
    # Write overall averages to the summary file
    for val in vals:
        k_values = []
        avg_unweighted = []
        avg_weighted = []
        
        summary.write("\nAverage Times for val = %s Across All Queries:\n" % val)
        for k in ks:
            k_values.append(k)
            unweighted_avg = sum(k_times[(k, val)]['unweighted']) / len(k_times[(k, val)]['unweighted']) if k_times[(k, val)]['unweighted'] else 0
            weighted_avg = sum(k_times[(k, val)]['weighted']) / len(k_times[(k, val)]['weighted']) if k_times[(k, val)]['weighted'] else 0
            
            avg_unweighted.append(unweighted_avg)
            avg_weighted.append(weighted_avg)
            
            summary.write("(%i, \t%f)\t(%i, \t%f)\n" % (k, unweighted_avg, k, weighted_avg))

    print("Got here!")
    # Close files before exiting
    summary.close()
    exit()

# Constructs SQL join queries for testing based on given vals and sch_vals
def constructQueries(vals):
    result = []
    sch_vals = ['1', '2', '3'] # Different shuffles to test for each val
    for val in vals:
        for sch_val in sch_vals:
            # Constructs and appends the query, val, and sch_val as a tuple to the result list
            # Q12 - TPCH
            result.append(("select o_orderkey, l_orderkey from order1%s%s join lineitem1%s%s on o_orderkey = l_orderkey limit 100000;" % (sch_val, val, sch_val, val), val, sch_val))
    return result

# Measures the running time of a join query for a given value of k
def measureTimeForKs(conn, joinQuery, ks, sigma, data_filename, iteration):
    f = None
    cur = None
    #joinTime = 0 # bha-add
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
        cur.execute('set work_mem = "64kB";')
        cur.execute('set statement_timeout = 1800000;') #1800000 = 30 mins
        
        # Log start of test run
        f.write("======================================================== \n")
        f.write("Time of the test run: " + str(datetime.datetime.now()) + '\n')
        f.write("BNL: ")
        f.write(joinQuery + " #" + str(iteration + 1) + '\n')

        # Execute the join query
        cur = conn.cursor('cur_uniq')

        cur.itersize = 1
        start = time()

        cur.execute(joinQuery)
        
        # Measure and log time before fetch
        f.write('  time before fetch: %f sec' % (time() - start))
        fetched = int(0)
        start = time()
        prev = start
        factor = sigma
        weightedTime = 0
        res = []
        
        # Fetch results and measure weighted time
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
            if joinTime >= 60: #900 = 15 min, 1500 = 25 min
                break
                
        # Additional logging for last fetched count if not in ks
        if fetched not in ks:
            res.append([fetched, joinTime, weightedTime])
            f.write("Final fetch count before exit: %d, %f, %f\n" % (fetched, joinTime, weightedTime))
            
        # Log total joined tuples and current query run time
        f.write("Total joined tuples fetched: %d,%f\n" % (fetched, joinTime))
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
