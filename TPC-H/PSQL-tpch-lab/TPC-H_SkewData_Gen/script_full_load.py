# !/bin/python
import datetime
import psycopg2
from time import time
from time import sleep
import sys

def main():
    zvals = ['0', '1', '1_5']
    shuffles = ['1', '3', '2']
    # Open connection
    conn = psycopg2.connect(host="/tmp/", database="mettas", user="mettas", port="1997")
    cur = conn.cursor()
    # If only one argument, clean is provided, drop and remake tables, then exit
    #if (len(sys.argv) == 2 and sys.argv[1] == "clean"):
    createRandomTables(conn, zvals)
    createTables(shuffles, zvals, conn)
    exit()

def makeCopyString(table, val):
    print("Time of data reload start: " + str(datetime.datetime.now()) + '\n')
    return "copy %s1%s from '/data/mettas/data/TPC-H/1/Z%s/%s.tbl' WITH DELIMITER AS '|';" %(table, val, val, table)

def makeRandomCopyString(scf, val, table):
    print("Time of data reload start: " + str(datetime.datetime.now()) + '\n')
    table_with_10 = table + "1"
    return "CREATE TABLE %s%s%s AS SELECT * FROM %s%s ORDER BY random();"%(table_with_10, scf, val, table, val)

def createRandomTables(conn, z_vals):
    cur = conn.cursor()

    for val in z_vals:
        print("Creating basic tables")
        # cur.execute("CREATE TABLE PART1" + val +""" ( P_PARTKEY     INTEGER NOT NULL,
        #                     P_NAME        VARCHAR(55) NOT NULL,
        #                     P_MFGR        CHAR(25) NOT NULL,
        #                     P_BRAND       CHAR(10) NOT NULL,
        #                     P_TYPE        VARCHAR(25) NOT NULL,
        #                     P_SIZE        INTEGER NOT NULL,
        #                     P_CONTAINER   CHAR(10) NOT NULL,
        #                     P_RETAILPRICE DECIMAL(15,2) NOT NULL,
        #                     P_COMMENT     VARCHAR(23) NOT NULL ,
        #                     extra_field   TEXT);""")
        cur.execute("CREATE TABLE SUPPLIER1"+ val + """ ( S_SUPPKEY     INTEGER NOT NULL,
                                S_NAME        CHAR(25) NOT NULL,
                                S_ADDRESS     VARCHAR(40) NOT NULL,
                                S_NATIONKEY   INTEGER NOT NULL,
                                S_PHONE       CHAR(15) NOT NULL,
                                S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                                S_COMMENT     VARCHAR(101) NOT NULL,
                                extra_field   TEXT);""")
        # cur.execute("CREATE TABLE LINEITEM1"+ val  + """ ( L_ORDERKEY    INTEGER NOT NULL,
        #                         L_PARTKEY     INTEGER NOT NULL,
        #                         L_SUPPKEY     INTEGER NOT NULL,
        #                         L_LINENUMBER  INTEGER NOT NULL,
        #                         L_QUANTITY    DECIMAL(15,2) NOT NULL,
        #                         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
        #                         L_DISCOUNT    DECIMAL(15,2) NOT NULL,
        #                         L_TAX         DECIMAL(15,2) NOT NULL,
        #                         L_RETURNFLAG  CHAR(1) NOT NULL,
        #                         L_LINESTATUS  CHAR(1) NOT NULL,
        #                         L_SHIPDATE    DATE NOT NULL,
        #                         L_COMMITDATE  DATE NOT NULL,
        #                         L_RECEIPTDATE DATE NOT NULL,
        #                         L_SHIPINSTRUCT CHAR(25) NOT NULL,
        #                         L_SHIPMODE     CHAR(10) NOT NULL,
        #                         L_COMMENT      VARCHAR(44) NOT NULL,
        #                         extra_field   TEXT);""")
        # cur.execute("CREATE TABLE ORDER1"+ val  + """ ( O_ORDERKEY       INTEGER NOT NULL,
        #                     O_CUSTKEY        INTEGER NOT NULL,
        #                     O_ORDERSTATUS    CHAR(1) NOT NULL,
        #                     O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
        #                     O_ORDERDATE      DATE NOT NULL,
        #                     O_ORDERPRIORITY  CHAR(15) NOT NULL,
        #                     O_CLERK          CHAR(15) NOT NULL,
        #                     O_SHIPPRIORITY   INTEGER NOT NULL,
        #                     O_COMMENT        VARCHAR(79) NOT NULL,
        #                     extra_field   TEXT);""")
        cur.execute("CREATE TABLE CUSTOMER1"+ val  + """( C_CUSTKEY     INTEGER NOT NULL,
                                C_NAME        VARCHAR(25) NOT NULL,
                                C_ADDRESS     VARCHAR(40) NOT NULL,
                                C_NATIONKEY   INTEGER NOT NULL,
                                C_PHONE       CHAR(15) NOT NULL,
                                C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                                C_MKTSEGMENT  CHAR(10) NOT NULL,
                                C_COMMENT     VARCHAR(117) NOT NULL,
                                extra_field   TEXT);""")
        cur.execute("CREATE TABLE PARTSUPP1"+ val  + """ ( PS_PARTKEY     INTEGER NOT NULL,
                                PS_SUPPKEY     INTEGER NOT NULL,
                                PS_AVAILQTY    INTEGER NOT NULL,
                                PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                                PS_COMMENT     VARCHAR(199) NOT NULL ,
                                extra_field   TEXT);""")

        # cur.execute(makeCopyString("part", val))
        # cur.execute(makeCopyString("order", val))
        # cur.execute(makeCopyString("lineitem", val))
        cur.execute(makeCopyString("supplier", val))
        cur.execute(makeCopyString("customer", val))
        cur.execute(makeCopyString("partsupp", val))

    conn.commit()

def createTables(shuffles, zvals, conn):
    cur = conn.cursor()

    for scf in shuffles:
        print("Recreating shuffle: %s" % scf)
        for val in zvals:
            print("Recreating zval: %s" % val)
            cur.execute("DROP TABLE IF EXISTS PART1" + scf + val + ";")
            print("Dropped PART1")
            cur.execute("DROP TABLE IF EXISTS SUPPLIER1" + scf + val + ";")
            print("Dropped SUPPLIER1")
            cur.execute("DROP TABLE IF EXISTS ORDER1" + scf + val + ";")
            print("Dropped ORDER1")
            cur.execute("DROP TABLE IF EXISTS LINEITEM1" + scf + val + ";")
            print("Dropped LINEITEM1")
            cur.execute("DROP TABLE IF EXISTS CUSTOMER1" + scf + val + ";")
            print("Dropped CUSTOMER1")
            cur.execute("DROP TABLE IF EXISTS PARTSUPP1" + scf + val + ";")
            print("Dropped PARTSUPP1")

            cur.execute(makeRandomCopyString(scf, val, "part"))
            cur.execute(makeRandomCopyString(scf, val, "order"))
            cur.execute(makeRandomCopyString(scf, val, "lineitem"))
            cur.execute(makeRandomCopyString(scf, val, "supplier"))
            cur.execute(makeRandomCopyString(scf, val, "customer"))
            cur.execute(makeRandomCopyString(scf, val, "partsupp"))

            conn.commit()

if __name__ == '__main__':
    main()
