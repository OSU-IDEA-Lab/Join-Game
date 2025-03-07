import psycopg2
from random import shuffle

# Database connection parameters
# host = "your_host"
# database = "your_database"
# user = "your_username"
# password = "your_password"

# Connect to the database
conn = psycopg2.connect(host="/tmp/", database="mettas", user="mettas", port="1997")
cursor = conn.cursor()

# Define table names
table1 = "books_review"
table2 = "temp1"

# Get all rows from table1
cursor.execute(f"SELECT * FROM {table1}")
rows = cursor.fetchall()

# Shuffle rows randomly
shuffle(rows)

# Insert the first 20,000 rows into table2
insert_query = f"INSERT INTO {table2} VALUES ({'%s, ' * (len(rows[0]) - 1)}%s)"

for row in rows[:20000]:
    cursor.execute(insert_query, row)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
