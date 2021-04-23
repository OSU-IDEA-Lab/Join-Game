#!/bin/python
# test
import pdb
import psycopg2
import re
from time import time
from datetime import datetime
import csv 
import sys

def main():
    # python filename jointype zvalue sigma
    # jointype: 1, Bandit; 0, Block
    t = 1 # int(sys.argv[1])
    z = 1 # int(sys.argv[2])

    # Exploration first full-join expt. cursors
    sigma = 0.99 # float(sys.argv[3])
    size = "0dot01"
    zValues = ["0"] # ["0dot5", "1", "1dot5", "2", "2dot5", "3"]
    kValues = [20, 50, 500, 1000, 50000]   # None for full-join


    joinQueryz1 = 'select * from order_s{s}_znew{z} join lineitem_s{s}_znew{z} on o_orderkey = l_orderkey;'
    joinQueryz1 = 'select * from order_s{s}_znew{z} join lineitem_s{s}_znew{z} on o_orderkey = l_orderkey;'
    joinQueryz2 = 'select * from order_s{s}_znew{z} join lineitem_s{s}_znew{z} on o_orderkey = l_orderkey;'
    joinQueryz3 = 'select * from order_s{s}_znew{z} join lineitem_s{s}_znew{z} on o_orderkey = l_orderkey;'

    if z == 1:
        joinQuery = joinQueryz1
    elif z == 2:
        joinQuery = joinQueryz2
    elif z == 3:
        joinQuery = joinQueryz3  

    if t == 0: 
        join = False
    else: 
        join = True
        
    # with psycopg2.connect(host="localhost", database="xiemian", user="xiemia", port="5444") as conn:
    conn = psycopg2.connect(host="localhost", database="tpch", user="daminens", port="5555")
    print("connect successfully",conn)
    loop = 1 
    for z in zValues:
        # banditSum = [0.0] * len(kValues)
        # nestedSum = [0.0] * len(kValues)
        for _ in range(loop):
            # prep(conn, z, '1') # this line shuffles lineitem table
            joinQuery = joinQuery.format(s=size, z=z)
            timeForKs = measureTimeForKs(conn, kValues, join, joinQuery,sigma, z)
            # nestedSum = [x + y for x, y in zip(nestedSum, timeForKs)]
            # timeForKs = measureTimeForKs(conn, kValues, False, joinQuery)
            # banditSum = [x + y for x, y in zip(banditSum, timeForKs)]
        # nestedJoinTime[z] = [s / loop for s in nestedSum]
        # banditJoinTime[z] = [s / loop for s in banditSum]
    # print('\n')
    # for z in zValues:
    #     print('z: ' + z)
    #     print('join time')
    #     print('nested:' + str(nestedJoinTime[z]))
    #     print('bandit:' + str(banditJoinTime[z]))
    #     print('==========')
    # for z in zValues:
    #     print(','.join(map(str, nestedJoinTime[z])))
    #     print(','.join(map(str, banditJoinTime[z])))

def prep(conn, z, s):
    prepLineitem(conn, z, s)
    # prepOrder(conn, z, s)

# creates a shuffled copy of lineitem table 
def prepLineitem(conn, z, s):
    print('Prep with z: ' + str(z) + ' s: ' + str(s))
    tableName  = 'lineitem_s' + str(s) + '_znew' + str(z)
    dropLineitemTable = 'drop table if exists tmp_lineitem;'
    createLineitemTable = 'create table tmp_lineitem as select * from ' + tableName  + ' order by random();'
    with conn.cursor() as cur:
        print('  updating lineitem table')
        cur.execute(dropLineitemTable)
        cur.execute(createLineitemTable)

# creates a shuffled copy of order table 
def prepOrder(conn, z, s):
    print('Prep with z: ' + str(z) + ' s: ' + str(s))
    orderTableName = 'order_s' + str(s) + '_z' + str(z)
    dropOrderTable = 'drop table if exists tmp_order;'
    createOrderTable = 'create table tmp_order as select * from ' + orderTableName + ' order by random();'
    with conn.cursor() as cur:
        print('  updating tables')
        cur.execute(dropOrderTable)
        cur.execute(createOrderTable)

# measures the running time of a join query for a given value of k 
def measureTimeForKs(conn, kValues, useBanditJoin, joinQuery,sigma, zVal):
    cur = conn.cursor()
    # with conn.cursor() as cur:
    if (useBanditJoin):
        print("Bandit Join")
        cur.execute('set enable_material=off;')
        cur.execute('set max_parallel_workers_per_gather=0;')
        cur.execute('set enable_hashjoin=off;')
        cur.execute('set enable_mergejoin=off;')
        cur.execute('set enable_indexonlyscan=off;')
        cur.execute('set enable_indexscan=off;')
        cur.execute('set enable_block=on;')
        cur.execute('set enable_bitmapscan=off;')
        # cur.execute('set work_mem="64kB";')
        cur.execute('set enable_fastjoin=on;')
        cur.execute('set enable_seqscan=on;')
        cur.execute('set enable_fliporder=off;')
    else:
        print("Block Nest Loop Join")
        cur.execute('set enable_material=off;')
        cur.execute('set max_parallel_workers_per_gather=0;')
        cur.execute('set enable_hashjoin=off;')
        cur.execute('set enable_mergejoin=off;')
        cur.execute('set enable_indexonlyscan=off;')
        cur.execute('set enable_indexscan=off;')
        cur.execute('set enable_block=on;')
        cur.execute('set enable_bitmapscan=off;')
        # cur.execute('set work_mem="64kB";')
        cur.execute('set enable_fastjoin=off;')
        cur.execute('set enable_seqscan=on;')
        cur.execute('set enable_fliporder=off;')

    with conn.cursor() as cur:
    # cur = conn.cursor("cur_uniq")
        cur.itersize = 1
        start = time()
        print(joinQuery)
        print("start time",start)
        cur.execute(joinQuery)
        with open("/postgres/dspr-psql/Join-Game/bandit_join_cur_execute_finished.txt", "w+") as out:
            out.write(datetime.now().strftime('%Y%m%d_%H:%M:%S'))
        print('  time before fetch: %f sec' % (time() - start))
        fetched = 0
        start = time()
        prev = start
        factor = sigma
        weightedTime = 0
        # iteration = 0
        res = list()
        for k in kValues:
            while (fetched < int(k)):
                row = cur.fetchone()
                # print(queryOutput)
                fetched += 1
                current = time()
                weightedTime += (current-prev)*factor
                prev = current
                factor *= sigma
                if fetched in kValues:
                    joinTime = current - start
                    print(zVal, fetched,joinTime,weightedTime)
                
                if row is None:
                    joinTime = current - start
                    print(zVal, "{} (full-join)".format(fetched), joinTime, weightedTime)         
                # elif fetched % 200 == 0:
                #     print(fetched,joinTime)
                
            joinTime = time() - start
            # print('  time for k = %s is %.2f sec' % (k, joinTime))
            res.append(joinTime * 1000)

        while row is not None:
            row = cur.fetchone()
            fetched += 1
            current = time()
            weightedTime += (current-prev)*factor
            prev = current
            factor *= sigma

        else:
            joinTime = current - start
            print(zVal, "{} (full-join)".format(fetched), joinTime, weightedTime)   

    with open("/postgres/dspr-psql/Join-Game/bandit_join_all_rows_fetched.txt", "w+") as out:
        out.write(datetime.now().strftime('%Y%m%d_%H:%M:%S'))
    return res

if __name__ == '__main__':
    main()


