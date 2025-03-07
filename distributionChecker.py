import psycopg2
import csv

def main():
    try:
        # Establish database connection
        conn = psycopg2.connect(
            host="/tmp/",
            database="mettas",
            user="mettas",
            port="1997"
        )
        cur = conn.cursor()

        # Optimized query to get counts of each make
        query = ("SELECT knownfortitles, COUNT(*) FROM actors GROUP BY knownfortitles;")
        cur.execute(query)
        results = cur.fetchall()
        sorted_results = sorted(results, key=lambda x: x[1], reverse=True)

        # Write the results to a CSV file
        with open('/data/mettas/data/actors_distribution.csv', 'w') as csvfile:  # Note 'wb' for Python 2.7
            writer = csv.writer(csvfile)
            writer.writerow(['make', 'count'])
            writer.writerows(sorted_results)

        print ("Results have been written to car_accidents.csv")

    except psycopg2.Error as db_err:
        print ("Database error occurred: {0}".format(db_err))
    except Exception as e:
        print ("An error occurred: {0}".format(e))
    finally:
        # Ensure resources are closed properly
        cur.close()
        conn.close()

def dist_comparison(file_a, file_b):
    # Output file path
    output_file_path = '/data/mettas/Join-Game/spotify_comparison.csv'
    
    # Prepare to read the first file and index its rows by 'make'
    with open(file_a, 'rb') as csv1:
        reader1 = csv.DictReader(csv1)
        rows1 = {}
        for row in reader1:
            make = row['make'].strip()
            rows1.setdefault(make, []).append(row)

    # Open the output file for writing
    with open(output_file_path, 'wb') as write_file:
        writer = csv.writer(write_file)
        
        # Prepare to read the second file and compare rows
        with open(file_b, 'rb') as csv2:
            reader2 = csv.DictReader(csv2)
            fieldnames1 = reader1.fieldnames
            fieldnames2 = reader2.fieldnames
            
            # Write combined header to the output file
            writer.writerow(fieldnames1 + fieldnames2)

            # Iterate over rows in file_b and check if the 'make' exists in rows indexed from file_a
            for row2 in reader2:
                make = row2['make'].strip()
                if make in rows1:
                    # For each matching 'make', combine and write rows from both files
                    for row1 in rows1[make]:
                        combined_row = [row1[field] for field in fieldnames1] + [row2[field] for field in fieldnames2]
                        writer.writerow(combined_row)

if __name__ == "__main__":
    # dist_comparison('/data/mettas/data/sp_artist_count.csv', '/data/mettas/data/sp_tracks_count.csv')
    main()
