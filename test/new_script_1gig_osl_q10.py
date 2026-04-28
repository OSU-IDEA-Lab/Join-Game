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
    conn = psycopg2.connect(host="/tmp/", database="tpch01g", user="jinjo", port="1531")
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

    def constructQueries(vals):
    result = []
    for val in vals:
        # Use explicit schema qualification for uniform and skewed datasets
        # Note: TPC-H uses 'orders', not 'order'
        query = f"""
            SELECT * FROM z{val}.customer, z{val}.orders 
            WHERE c_custkey = o_custkey 
            LIMIT 100000;
        """
        # Pass 'val' and a dummy 'sch_val' to match the loop signature in main()
        result.append((query, val, "1"))
    return result

    def measureTimeForKs(conn, joinQuery, ks, sigma, data_filename, iteration):
        res = []
        f = None
        cur = None
        
        try:
            # 1. Setup session parameters using a standard cursor
            setup_cur = conn.cursor()
            setup_cur.execute('SET enable_material=off;')
            setup_cur.execute('SET max_parallel_workers_per_gather=0;')
            setup_cur.execute('SET enable_hashjoin=off;')
            setup_cur.execute('SET enable_mergejoin=off;')
            setup_cur.execute('SET enable_indexonlyscan=off;')
            setup_cur.execute('SET enable_indexscan=off;')
            setup_cur.execute('SET enable_block=off;')
            setup_cur.execute('SET enable_bitmapscan=off;')
            setup_cur.execute('SET enable_fastjoin=off;')
            setup_cur.execute('SET enable_seqscan=off;')
            setup_cur.execute('SET enable_fliporder=off;')
            setup_cur.execute('SET enable_nestloop=on;')
            setup_cur.execute("SET work_mem = '64kB';")
            setup_cur.execute('SET statement_timeout = 1800000;')
            setup_cur.close()

            # 2. Open log file
            f = open(data_filename, 'a')
            f.write("======================================================== \n")
            f.write(f"Run: {datetime.datetime.now()} | Iteration: {iteration + 1}\n")

            # 3. Use the named cursor for the qualified query
            cur = conn.cursor('cur_uniq')
            cur.itersize = 1 
            
            start = time()
            cur.execute(joinQuery) 
            
            f.write('  time before fetch: %f sec\n' % (time() - start))
            
            # 4. Measure progressive timing
            fetched = 0
            start = time()
            prev = start
            factor = sigma
            weightedTime = 0
            barrier = 50
            
            print(f"Query started: {joinQuery.strip()[:60]}...")

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
                if joinTime >= 60: 
                    break
                    
            if fetched not in ks:
                res.append([fetched, joinTime, weightedTime])
                
            f.write(f"Total tuples: {fetched} | Time: {joinTime:.2f}s\n")

        except Exception as e:
            print(f"Error executing query: {e}")
            if conn:
                conn.rollback() # Reset transaction state
        finally:
            # SAFE CLOSING: Catch the ProgrammingError if the cursor is already invalid
            if cur is not None:
                try:
                    cur.close()
                except psycopg2.ProgrammingError:
                    pass 
            if f is not None:
                f.flush()
                f.close()

        if not res:
            res.append([0, 0, 0])
        
        conn.commit() # Save settings for the next iteration
        return res
        
if __name__ == '__main__':
    main()
