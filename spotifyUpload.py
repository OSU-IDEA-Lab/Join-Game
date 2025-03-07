import csv
import psycopg2
import ast
from datetime import datetime

def connect_db():
    conn = psycopg2.connect(host="/tmp/", database="mettas", user="mettas", port="1997")
    return conn


def insert_artists(filename):
    a = 1
    conn = connect_db()
    cur = conn.cursor()
    try:
        with open(filename, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                artist_id = row[0]
                artist_name = row[1]
                # Ensure date format matches your CSV format
                cur.execute("INSERT INTO artists (artist_id, artist_name) VALUES (%s, %s)", (artist_id, artist_name))
                print(a)
                a+=1
                conn.commit()
        print("Artists inserted successfully.")
    except Exception as error:
        print("Error: ", error)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def insert_tracks(filename):
    conn = connect_db()
    cur = conn.cursor()
    row_number = 1
    try:
        # Try different encoding if 'utf-8' fails, e.g., 'ISO-8859-1'
        with open(filename, 'rb') as file:
            reader = csv.reader((line.decode('ISO-8859-1').encode('utf-8') for line in file))
            next(reader)  # Skip the header row
            for row in reader:
                try:
                    # Assume the row is properly encoded now
                    artists = row[4].strip("[]").replace("'", "").split(", ")
                    cur.execute("""
                    INSERT INTO spotify_tracks (id, name, album, album_id, artists, explicit, year)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                                (row[0], row[1], row[2], row[3], artists, row[5].lower() == 'true', row[6]))
                    conn.commit()
                    print(row_number)
                except Exception as inner_error:
                    print("Error inserting row {}: {}".format(row_number, inner_error))
                    conn.rollback()
                row_number += 1
        print("Finished inserting tracks.")
    except Exception as outer_error:
        print("Error processing file: ", outer_error)
    finally:
        cur.close()
        conn.close()

def process_tracks(input_file, output_file):
    with open(input_file, 'rb') as infile, open(output_file, 'wb') as outfile:
        reader = csv.DictReader(infile)
        # Update field names for the output file
        fieldnames = reader.fieldnames[:]
        artist_index = fieldnames.index('artists')
        artist_id_index = fieldnames.index('artist_ids')
        fieldnames[artist_index] = 'artist'
        fieldnames[artist_id_index] = 'artist_id'

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            try:
                # Try decoding with 'ISO-8859-1' if 'utf-8' fails
                artists = ast.literal_eval(row['artists'].decode('ISO-8859-1'))
                artist_ids = ast.literal_eval(row['artist_ids'].decode('ISO-8859-1'))
            except UnicodeDecodeError:
                # Fallback or log the error
                continue  # or handle the error as needed

            for artist, artist_id in zip(artists, artist_ids):
                new_row = dict(row)
                new_row['artists'] = artist
                new_row['artist_ids'] = artist_id
                new_row['artist'] = new_row.pop('artists')
                new_row['artist_id'] = new_row.pop('artist_ids')
                # Encode all fields before writing to file
                writer.writerow({k: v.encode('utf-8') if isinstance(v, unicode) else v for k, v in new_row.items()})




if __name__ == "__main__":
    # insert_artists('/data/mettas/data/sp_artist.csv')
    # process_tracks('/data/mettas/data/tracks_features.csv', '/data/mettas/data/sorted_tracks_features.csv')
    insert_tracks('/data/mettas/data/sorted_tracks_features.csv')
