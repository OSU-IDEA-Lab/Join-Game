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
        cur.execute("DROP TABLE IF EXISTS books_data" + scf + ";")
        cur.execute("DROP TABLE IF EXISTS reviews" + scf + ";")

        cur.execute("CREATE TABLE books_data" + scf + """ (   id INT,
                                                                    title TEXT,
                                                                    description TEXT,
                                                                    authors TEXT,
                                                                    image_url TEXT,
                                                                    preview_link TEXT,
                                                                    publisher TEXT,
                                                                    published_date DATE,
                                                                    info_link TEXT,
                                                                    categories TEXT
                                                                );""")
        cur.execute("CREATE TABLE reviews" + scf + """ (  id VARCHAR(13),
                                                                book_id TEXT,
                                                                title TEXT,
                                                                price DECIMAL(15, 2),
                                                                user_id VARCHAR(255),
                                                                profile_name TEXT,
                                                                helpfulness TEXT,
                                                                score DECIMAL(2, 1),
                                                                review_time TIMESTAMP,
                                                                summary TEXT,
                                                                text TEXT
                                                            );""")

        cur.execute(makeCopyString(scf, "books_data"))
        cur.execute(makeCopyString(scf, "reviews"))
        conn.commit()
        print('ABR done')

if __name__ == '__main__':
    main()
