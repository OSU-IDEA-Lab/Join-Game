import csv
import psycopg2
import re
from datetime import datetime

def connect_db(db, db_user, db_port):
    conn = psycopg2.connect(host="/tmp/", database=db, user=db_user, port=db_port)
    return conn

def parse_rating(rating_string):
    if not rating_string or rating_string == 'N/A':
        return None
    match = re.match(r'(\d+\.?\d*)', rating_string)
    return float(match.group(1)) if match else None

def parse_runtime(runtime_string):
    if not runtime_string or runtime_string == 'N/A':
        return None
    match = re.match(r'(\d+)', runtime_string)
    return int(match.group(1)) if match else None

def reset_omdb_table(db, db_user, db_port):
    conn = connect_db(db, db_user, db_port)
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS omdbMovies;")
        cur.execute("""
            CREATE TABLE omdbMovies (
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
            );
        """)
        conn.commit()
        print("'omdb' table reset successfully.")
    except Exception as e:
        print("Error resetting 'omdb' table")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def insert_omdb_data(filename, db, db_user, db_port):
    conn = connect_db(db, db_user, db_port)
    cur = conn.cursor()
    reset_omdb_table()
    row_number = 0
    inserted_count = 0
    updated_count = 0
    try:
        with open(filename, 'r') as file:
            reader = csv.DictReader(file)

            for row in reader:
                row_number += 1
                try:
                    data = [
                        int(row['id']),
                        row['imdbID'],
                        row['Title'],
                        int(row['Year']) if row['Year'] and row['Year'].isdigit() else None,
                        row['Rating'],
                        parse_runtime(row['Runtime']),
                        row['Genre'],
                        datetime.strptime(row['Released'], '%Y-%m-%d').date() if row['Released'] and row['Released'] != 'N/A' else None,
                        row['Director'],
                        row['Writer'],
                        row['Cast'],
                        parse_rating(row['imdbRating']),
                        row['Plot'],
                        row['Language'],
                        row['Country'],
                        row['Type']
                    ]

                    cur.execute("""
                        INSERT INTO omdbMovies (
                            id, imdbID, Title, Year, Rating, Runtime, Genre, Released,
                            Director, Writer, "Cast", imdbRating, Plot,
                            Language, Country, Type
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (id) DO UPDATE SET
                            imdbID = EXCLUDED.imdbID,
                            Title = EXCLUDED.Title,
                            Year = EXCLUDED.Year,
                            Rating = EXCLUDED.Rating,
                            Runtime = EXCLUDED.Runtime,
                            Genre = EXCLUDED.Genre,
                            Released = EXCLUDED.Released,
                            Director = EXCLUDED.Director,
                            Writer = EXCLUDED.Writer,
                            "Cast" = EXCLUDED."Cast",
                            imdbRating = EXCLUDED.imdbRating,
                            Plot = EXCLUDED.Plot,
                            Language = EXCLUDED.Language,
                            Country = EXCLUDED.Country,
                            Type = EXCLUDED.Type;
                    """, data)
                    
                    if cur.rowcount == 1:
                        inserted_count += 1
                    else:
                        updated_count += 1
                    
                    if row_number % 1000 == 0:
                        conn.commit()
                        print("Processed {} rows. Inserted: {}, Updated: {}".format(row_number, inserted_count, updated_count))

                except Exception as inner_error:
                    print("Error processing row {}: {}".format(row_number, inner_error))
                    conn.rollback()

        conn.commit()
        print("Finished processing OMDb data. Total rows: {}, Inserted: {}, Updated: {}".format(row_number, inserted_count, updated_count))
    except Exception as outer_error:
        print("Error processing file: {}".format(outer_error))
    finally:
        cur.close()
        conn.close()

def reset_imdb_table(db, db_user, db_port):
    conn = connect_db(db, db_user, db_port)
    cur = conn.cursor()
    try:
        # Drop the existing table
        cur.execute("DROP TABLE IF EXISTS imdb;")
        print("Existing 'imdb' table dropped.")

        # Recreate the table with extended field lengths
        cur.execute("""
            CREATE TABLE imdb (
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
            );
        """)
        print("'imdb' table recreated successfully with extended field lengths.")
        conn.commit()
    except Exception as e:
        print("Error resetting 'imdb' table: {}".format(e))
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def insert_imdb_data(filename, db, db_user, db_port):
    conn = connect_db(db, db_user, db_port)
    cur = conn.cursor()
    reset_imdb_table()
    row_number = 0
    try:
        with open(filename, 'r') as file:
            reader = csv.reader(file, quotechar='"', delimiter=',', quoting=csv.QUOTE_MINIMAL, skipinitialspace=True)
            next(reader)  # Skip the header row

            for row in reader:
                row_number += 1
                try:
                    # Extend the row with None values if it's shorter than 12 columns
                    row_data = row + [None] * (12 - len(row))
                    
                    imdbid = row_data[0] or None
                    if not imdbid:
                        print("Skipping row {}: Missing IMDb ID".format(row_number))
                        continue

                    # Prepare data for insertion
                    data = [
                        imdbid,
                        row_data[1] or None,  # title
                        int(row_data[2]) if row_data[2] and row_data[2].isdigit() else None,  # year
                        row_data[3] or None,  # genres
                        row_data[4] or None,  # director
                        row_data[5] or None,  # writer
                        row_data[6] or None,  # cast
                        int(row_data[7]) if row_data[7] and row_data[7].isdigit() else None,  # runtime
                        row_data[8] or None,  # country
                        row_data[9] or None,  # language
                        parse_rating(row_data[10]),  # rating
                        row_data[11] or None,  # plot
                    ]

                    # Insert new entry
                    cur.execute("""
                        INSERT INTO imdb (
                            imdbid, title, year, genres, director, writer, "cast", 
                            runtime, country, language, rating, plot
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, data)
                    print("Inserted row {}".format(row_number))
                    conn.commit()
                except Exception as inner_error:
                    print("Error processing row {}: {}".format(row_number, inner_error))
                    print("Row content: {}".format(row))
                    for i, item in enumerate(row):
                        print("Column {}: {}{}".format(i, item[:50], '...' if len(item) > 50 else ''))
                    conn.rollback()

        print("Finished processing IMDb data.")
    except Exception as outer_error:
        print("Error processing file: {}".format(outer_error))
    finally:
        cur.close()
        conn.close()

def create_actors(db, db_user, db_port):
    conn = connect_db(db, db_user, db_port)
    cur = conn.cursor()
    try:
        cur.execute('DROP TABLE IF EXISTS actors;')
        cur.execute("""
                        CREATE TABLE actors (
                            nconst TEXT,
                            primaryName TEXT,
                            birthYear TEXT,
                            deathYear TEXT,
                            primaryProfession TEXT,
                            knownForTitles TEXT
                        );
                    """)
        conn.commit()
        print("Actors table has been created")
    except Exception as e:
        print("Error with Actors Table: ", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def load_titles(filepath):
    titles_dict = {}
    with open(filepath, 'r', newline='') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            titles_dict[row['tconst']] = row['originalTitle']
    return titles_dict

def title_finder(ids, titles_dict):
    return [titles_dict.get(id) for id in ids.split(',') if titles_dict.get(id)]

def insert_actors(input_file_actors, db, db_user, db_port):
    conn = connect_db(db, db_user, db_port)
    cur = conn.cursor()
    create_actors()
    row_number = 0
    try:
        with open(input_file_actors, 'r', newline='') as file:
            reader = csv.DictReader(file, delimiter='\t')
            titles_dict = load_titles("/data/Titles.tsv")
            for row in reader:
                try:
                    data = [
                        row['nconst'],
                        row['primaryName'],
                        row['birthYear'],
                        row['deathYear'],
                        row['primaryProfession'],
                    ]
                    titles = title_finder(row['knownForTitles'], titles_dict)
                    for title in titles:
                        cur.execute("""
                                    INSERT INTO actors (
                                        nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s
                                    )
                                    """, data + [title])
                        row_number += 1
                        print(row_number)
                        if row_number % 1000 == 0:
                            conn.commit()
                            print("Inserted: ", row_number)
                except Exception as insertion_error:
                    print("error when inserting data: ", insertion_error)
                    conn.rollback()
        conn.commit()
        print('Data processing done.')
    except Exception as error:
        print("Error inserting data: ", error)

    finally:
        cur.close()
        conn.close()

def title_finder(ids, titles_dict):
    return [titles_dict.get(id) for id in ids.split(',') if titles_dict.get(id)]

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python script.py <database> <user> <port> <csv_filename>")
        sys.exit(1)

    # Read command-line arguments
    db = sys.argv[1]
    db_user = sys.argv[2]
    db_port = sys.argv[3]
    insert_omdb_data('/data/movies/fixed_omdb.csv', db, db_user, db_port)
    insert_imdb_data('/data/movies/imdb.csv', db, db_user, db_port)
    insert_actors("/data/Actors.tsv", db, db_user, db_port)
