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
            cur.execute("DROP TABLE IF EXISTS artists" + scf + ";")
            cur.execute("DROP TABLE IF EXISTS spotify_tracks" + scf + ";")

            cur.execute("CREATE TABLE artists" + scf + """ (
                                                        artist_id TEXT,
                                                        artist_name TEXT
                                                    );""")
            cur.execute("CREATE TABLE spotify_tracks" + scf + """ (
                                                        id TEXT,
                                                        name TEXT,
                                                        album TEXT,
                                                        album_id TEXT,
                                                        artists TEXT,
                                                        explicit BOOLEAN,
                                                        year INT
                                                    );""")

            # Execute and commit data copy for wdc1 and wdc2
            cur.execute(makeCopyString(scf, "artists"))
            cur.execute(makeCopyString(scf, "spotify_tracks"))
            conn.commit()
            print("Data loaded successfully for shuffle {}.".format(scf))
    except Exception as e:
        print("An error occurred: {}".format(e))
        conn.rollback()  # Rollback in case of error
        raise  # Optionally re-raise the error after handling
    print('Spotify done')


if __name__ == '__main__':
    main()
