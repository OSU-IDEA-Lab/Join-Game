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
            cur.execute("DROP TABLE IF EXISTS imdb" + scf + ";")
            cur.execute("DROP TABLE IF EXISTS omdbMovies" + scf + ";")
            cur.execute("DROP TABLE IF EXISTS actors" + scf + ";")

            cur.execute("CREATE TABLE imdb" + scf + """ (
                                                        imdbid VARCHAR(10) PRIMARY KEY,
                                                        title TEXT,
                                                        year INTEGER,
                                                        genres TEXT,
                                                        director TEXT,
                                                        writer TEXT,
                                                        "cast" TEXT,
                                                        runtime INTEGER,
                                                        country TEXT,
                                                        language TEXT,
                                                        rating NUMERIC(3,1),
                                                        plot TEXT
                                                    );""")
            cur.execute("CREATE TABLE omdbMovies" + scf + """ (
                                                        id INTEGER PRIMARY KEY,
                                                        imdbID VARCHAR(10),
                                                        Title TEXT,
                                                        Year INTEGER,
                                                        Rating TEXT,
                                                        Runtime INTEGER,
                                                        Genre TEXT,
                                                        Released DATE,
                                                        Director TEXT,
                                                        Writer TEXT,
                                                        "Cast" TEXT,
                                                        imdbRating NUMERIC(3,1),
                                                        Plot TEXT,
                                                        Language TEXT,
                                                        Country TEXT,
                                                        Type TEXT
                                                    );""")
            cur.execute("CREATE TABLE actors" + scf +""" (
                            nconst TEXT,
                            primaryName TEXT,
                            birthYear TEXT,
                            deathYear TEXT,
                            primaryProfession TEXT,
                            knownForTitles TEXT
                        );""")

            # Execute and commit data copy for wdc1 and wdc2
            cur.execute(makeCopyString(scf, "imdb"))
            cur.execute(makeCopyString(scf, "omdbMovies"))
            cur.execute(makeCopyString(scf, "actors"))
            conn.commit()
            print("Data loaded successfully for shuffle {}.".format(scf))
    except Exception as e:
        print("An error occurred: {}".format(e))
        conn.rollback()  # Rollback in case of error
        raise  # Optionally re-raise the error after handling
    print('movies done')

if __name__ == '__main__':
    main()
