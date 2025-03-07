# !/bin/python
import datetime
import psycopg2
from time import time
from time import sleep
import sys

def main():
    shuffles = ['1','2','3']
    # Open connection
    conn = psycopg2.connect(host="/tmp/", database="mettas", user="mettas", port="1997")
    cur = conn.cursor()
    # If only one argument, clean is provided, drop and remake tables, then exit
    if (len(sys.argv) == 2 and sys.argv[1] == "clean"):
        createTables(shuffles, conn)
        exit()

def makeCopyString(scf, table):
    print("Time of data reload start: " + str(datetime.datetime.now()) + '\n')
    ##### If you cannot access the below path, then download the data from the source https://github.com/sbharghav/1gig and change the below copy path accordingly
    return "INSERT INTO %s%s SELECT * FROM %s ORDER BY RANDOM();" % (table, scf, table)

def createTables(shuffles, conn):
    cur = conn.cursor()

    for scf in shuffles:
        print("Recreating shuffle: %s" % scf)
        cur.execute("DROP TABLE IF EXISTS gr_authors" + scf + ";")
        cur.execute("DROP TABLE IF EXISTS gr_books" + scf + ";")

        cur.execute("CREATE TABLE gr_books" + scf + """ (   id VARCHAR(10),
                                                                    title TEXT,
                                                                    author_name TEXT,
                                                                    language TEXT,
                                                                    original_publication_date DATE,
                                                                    description TEXT
                                                                );""")
        cur.execute("CREATE TABLE gr_authors" + scf + """ ( id TEXT,
                                                                name TEXT,
                                                                gender TEXT,
                                                                ratings_count INTEGER,
                                                                average_rating FLOAT,
                                                                text_reviews_count INTEGER,
                                                                works_count INTEGER,
                                                                fans_count INTEGER
                                                            );""")

        cur.execute(makeCopyString(scf, "gr_authors"))
        cur.execute(makeCopyString(scf, "gr_books"))
        conn.commit()
        print('GRBA done')


if __name__ == '__main__':
    main()
