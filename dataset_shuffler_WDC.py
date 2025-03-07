# !/bin/python
from __future__ import print_function
import datetime
import psycopg2
from time import time
from time import sleep
import sys

def main():
    shuffles = ['1', '2', '3']
    # Open connection
    conn = psycopg2.connect(host="/tmp/", database="mettas", user="mettas", port="1997")
    cur = conn.cursor()
    # If only one argument, clean is provided, drop and remake tables, then exit
    if len(sys.argv) == 2 and sys.argv[1] == "clean":
        createTables(shuffles, conn)
        exit()

def makeCopyString(scf, table):
    print("Time of data reload start: " + str(datetime.datetime.now()))
    # Ensure only entries with a non-null brand are selected and copied
    query = "INSERT INTO {}{} SELECT * FROM {} ORDER BY RANDOM();".format(table, scf, table)
    return query

def createTables(shuffles, conn):
    cur = conn.cursor()
    try:
        for scf in shuffles:
            print("Recreating shuffle: {}".format(scf))
            cur.execute("DROP TABLE IF EXISTS wdc1Brands" + scf + ";")
            cur.execute("DROP TABLE IF EXISTS wdc2Brands" + scf + ";")
            cur.execute("DROP TABLE IF EXISTS wdc3Brands" + scf + ";")

            cur.execute("CREATE TABLE wdc1Brands" + scf + """ (
                                                        id TEXT,
                                                        title TEXT,
                                                        description TEXT,
                                                        brand TEXT,
                                                        price TEXT,
                                                        category TEXT,
                                                        cluster_id TEXT
                                                    );""")
            cur.execute("CREATE TABLE wdc2Brands" + scf + """ (
                                                        id TEXT,
                                                        title TEXT,
                                                        description TEXT,
                                                        brand TEXT,
                                                        price TEXT,
                                                        category TEXT,
                                                        cluster_id TEXT
                                                    );""")
            cur.execute("CREATE TABLE wdc3Brands" + scf + """ (
                                                        id TEXT,
                                                        title TEXT,
                                                        description TEXT,
                                                        brand TEXT,
                                                        price TEXT,
                                                        category TEXT,
                                                        cluster_id TEXT
                                                    );""")

            # Execute and commit data copy for wdc1 and wdc2
            cur.execute(makeCopyString(scf, "wdc1Brands"))
            cur.execute(makeCopyString(scf, "wdc2Brands"))
            cur.execute(makeCopyString(scf, "wdc3Brands"))

            conn.commit()
            print("Data loaded successfully for shuffle {}.".format(scf))
    except Exception as e:
        print("An error occurred: {}".format(e))
        conn.rollback()  # Rollback in case of error
        raise  # Optionally re-raise the error after handling
    print('WDC done')

if __name__ == '__main__':
    main()
