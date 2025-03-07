import psycopg2
import os

# Function to log all print statements into a text file
class Logger:
    def __init__(self, file_name):
        self.file = open(file_name, 'w')

    def write(self, message):
        self.file.write(message + '\n')
        # Using print statement without parentheses for Python 2.7 compatibility
        print (message)

    def close(self):
        self.file.close()

# Input variables
vals = ['1', '2', '3']
lv = ['1', '2', '3', '4', '5']

# SQL query function
def query(val, dist):
    return (
        "SELECT Car_brands%s.make, parking_tickets%s.vehicle_make "
        "FROM Car_brands%s "
        "JOIN parking_tickets%s ON levenshtein(trim(Car_brands%s.make::varchar(10)), trim(parking_tickets%s.vehicle_make::varchar(10))) <= %s "
        "LIMIT 18800;" % (val, val, val, val, val, val, dist)
    )

# Initialize the logger
log_file = 'debugger_output_log.txt'
logger = Logger(log_file)

try:
    # Log the query and inputs
    for val in vals:
        for dist in lv:
            logger.write("Running SQL query with inputs:")
            logger.write("val: %s" % val)
            logger.write("dist: %s" % dist)
            logger.write(query(val, dist))

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(host="/tmp/", database="mettas", user="mettas", port="1997")
    cur = conn.cursor()

    # Execute the query
    logger.write("Executing query...")
    cur.execute(query(val, dist))

    # Fetch the result
    results = cur.fetchall()

    # Log the results
    logger.write("Number of rows returned: %d" % len(results))
    for row in results:
        logger.write(str(row))

    # Close the cursor and the connection
    cur.close()
    conn.close()
    logger.write("Database connection closed.")

except Exception as e:
    # Log any exceptions
    logger.write("An error occurred: %s" % str(e))

finally:
    # Close the log file
    logger.close()

# Notify user where the output is stored
print ("Output and logs written to %s" % log_file)
