#!/bin/python
import datetime
import psycopg2
from time import time
import sys

def main():
    # Define the range of k values and the sigma for weighted timing calculations
    ks = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000, 17500, 20000, 22500, 25000, 30000, 35000, 40000, 45000, 50000, 60000, 70000, 80000, 90000, 100000]
    sigma = .99
    vals = ['0', '1', '1_5']
    sizes = ['01', '1', '10']
    
    # Expecting two command-line arguments for output filenames
    if (len(sys.argv) != 3):
        print("Expecting 2 arguments, output filename and summary filename suffixes")
        exit()

    # Open files for writing
    data_filename = str(sys.argv[1])
    summary_filename = str(sys.argv[2])

    for size in sizes:
        # Establish database connection
        conn = psycopg2.connect(host="/tmp/", database="tpch"+size+"g", user="jinjo", port="1531")
        cur = conn.cursor()
        summary = open("test/q2_"+size+"g_tpch_"+summary_filename+"_summary.txt", 'w+')

        clear_output = "test/q2_"+size+"g_tpch_"+data_filename+"_output.txt"
        open(clear_output, 'w').close()

        # Generate SQL join queries for testing
        joinQueries = constructQueries(vals)

        # Initialize data structure for storing timing data
        k_times = {(k, val): {'unweighted': [], 'weighted': []} for k in ks for val in vals}
        k_times.update({('others', val): {'unweighted': [], 'weighted': []} for val in vals})


        # Execute each generated query and measure performance
        loop = 1
        for joinQuery, val, sch_val in joinQueries:
            timeForKs = []
            
            # Disable the statement timeout for long-running queries
            cur.execute('set statement_timeout = 0;')
            
            for i in range(loop):
                print("Running this query for the " + str(i) + " time(s)")
                timeForKs.append(measureTimeForKs(conn, joinQuery, ks, sigma, data_filename, i, val, size))
                
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

                if kCurr in ks:
                    target_key = (kCurr, val)
                else:
                    target_key = ('others', val)
            
                summary.write("K val:%i\tAverage time (unweighted): %f\t Average time (weighted): %f\n" % (
                kCurr, normalsum / len(timeForKs), weightedsum / len(timeForKs)))
                k_times[target_key]['unweighted'].append(normalsum / len(timeForKs))
                k_times[target_key]['weighted'].append(weightedsum / len(timeForKs))
            summary.flush()
            
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
        summary.close()
    exit()

def constructQueries(vals):
    result = []
    for val in vals:
        query = f"""
            SELECT * FROM z{val}.part, z{val}.supplier, z{val}.partsupp 
            WHERE p_partkey = ps_partkey 
              AND s_suppkey = ps_suppkey 
            LIMIT 100000;
        """
        result.append((query, val, "1")) 
    return result

def measureTimeForKs(conn, joinQuery, ks, sigma, data_filename, iteration, val, size):
    res = []
    f = None
    cur = None
    mj_cur = None
    
    try:
        # 1. Setup session parameters for MERGE JOIN Baseline
        setup_cur = conn.cursor()
        setup_cur.execute('SET enable_material=off;')
        setup_cur.execute('SET max_parallel_workers_per_gather=0;')
        setup_cur.execute('SET enable_hashjoin=off;')
        setup_cur.execute('SET enable_mergejoin=on;')
        setup_cur.execute('SET enable_indexonlyscan=off;')
        setup_cur.execute('SET enable_indexscan=off;')
        setup_cur.execute('SET enable_block=off;')
        setup_cur.execute('SET enable_bitmapscan=off;')
        setup_cur.execute('SET enable_seqscan=on;')
        setup_cur.execute('SET enable_nestloop=off;')
        setup_cur.execute("SET work_mem = '64kB';")
        setup_cur.execute('SET statement_timeout = 1800000;')
        setup_cur.close()

        # 2. Open log file
        f = open("test/q2_"+size+"g_tpch_"+data_filename+"_output.txt", 'a')
        f.write("======================================================== \n")
        f.write(f"Run: {datetime.datetime.now()} | Schema: z{val} | Iteration: {iteration + 1}\n")

        # 3. Execute MERGE JOIN and record totals
        print(f"Query started (Merge Join Baseline for z{val}): {joinQuery.strip()[:60]}...")
        mj_cur = conn.cursor('mj_cur')
        mj_cur.itersize = 2000
        
        mj_start = time()
        mj_cur.execute(joinQuery)
        
        # Fast iteration to count rows without storing them all in python memory
        mj_fetched = sum(1 for _ in mj_cur)
        mj_time = time() - mj_start
        mj_cur.close()

        # Log Merge Join results to the file directly above the EHJ data
        f.write(f"Merge join total tuples: {mj_fetched} | Time: {mj_time:.2f}s\n\n")

        # 4. Setup session parameters for HASH JOIN
        setup_cur = conn.cursor()
        setup_cur.execute('SET enable_mergejoin=off;')
        setup_cur.execute('SET enable_hashjoin=on;')
        
        # Dynamically scale work_mem based on the TPC-H size being tested
        if size == '01':
            setup_cur.execute("SET work_mem = '4MB';")   # Forces spilling on 0.1GB without thrashing
        elif size == '1':
            setup_cur.execute("SET work_mem = '16MB';")  # Forces spilling on 1GB
        elif size == '10':
            setup_cur.execute("SET work_mem = '128MB';") # Forces spilling on 10GB
            
        setup_cur.close()

        # 5. Execute HASH JOIN with progressive Time-to-First-K tracking
        cur = conn.cursor('cur_uniq')
        cur.itersize = 1 
        
        print(f"Query started (Hash Join Target for z{val}): {joinQuery.strip()[:60]}...")
        start = time()
        cur.execute(joinQuery) 
        
        f.write('  time before fetch: %f sec\n' % (time() - start))
        
        fetched = 0
        start = time()
        prev = start
        factor = sigma
        weightedTime = 0
        barrier = 50
        
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
                
            while conn.notices:
                notice = conn.notices.pop(0)
                f.write(f"SERVER INFO: {notice}")
                
        if fetched not in ks:
            res.append([fetched, joinTime, weightedTime])
            
        f.write(f"EHJ Total tuples: {fetched} | Time: {joinTime:.2f}s\n")

    except Exception as e:
        print(f"Error in z{val} loop: {e}")
        if conn:
            conn.rollback() 
    finally:
        if cur is not None:
            try:
                cur.close()
            except psycopg2.ProgrammingError:
                pass 
        if mj_cur is not None:
            try:
                mj_cur.close()
            except psycopg2.ProgrammingError:
                pass
        if f is not None:
            f.flush()
            f.close()

    if not res:
        res.append([0, 0, 0])
    
    conn.commit()
    return res

if __name__ == '__main__':
    main()