import json
import psycopg2
import pandas as pd
import re

# Load JSON data
file_path = '/data/mettas/data/offers_consWgs_english.json'
with open(file_path, 'r') as file:
    data = [json.loads(line) for line in file]

# Normalize JSON data and create a DataFrame
records = []
for entry in data:
    record = {
        "url": entry.get("url"),
        "id": entry.get("cluster_id"),
        "productID": next((i.get("/productID") for i in entry.get("identifiers", []) if "/productID" in i), None),
        "name": next((i.get("/name") for i in entry.get("schema.org_properties", []) if "/name" in i), None),
        "description": next((i.get("/description") for i in entry.get("schema.org_properties", []) if "/description" in i), None),
        # Extract only the numeric part of the price
        "price": next((re.search(r'\d+\.?\d*', i.get("/price")).group(0) if re.search(r'\d+\.?\d*', i.get("/price")) else None for i in entry.get("schema.org_properties", []) if "/price" in i), None),
        "priceCurrency": next((i.get("/priceCurrency") for i in entry.get("schema.org_properties", []) if "/priceCurrency" in i), None),
        "brand": next((i.get("/brand") for i in entry.get("schema.org_properties", []) if "/brand" in i), None),
        "manufacturer": next((i.get("/manufacturer") for i in entry.get("schema.org_properties", []) if "/manufacturer" in i), None),
    }
    records.append(record)

df = pd.DataFrame(records)

# Database connection details
conn = psycopg2.connect(
    dbname="mettas",
    user="mettas",
    host="/tmp/",
    port="1997"
)
cur = conn.cursor()

# Split the DataFrame into two
half_index = len(df) // 2
df1 = df.iloc[:half_index]
df2 = df.iloc[half_index:]

# Insert first half into fullWDC1
for index, row in df1.iterrows():
    cur.execute("""
        INSERT INTO fullWDC1 (url, id, productID, name, description, price, priceCurrency, brand, manufacturer)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (row['url'], row['id'], row['productID'], row['name'], row['description'], row['price'], row['priceCurrency'], row['brand'], row['manufacturer']))

# Insert second half into fullWDC2
for index, row in df2.iterrows():
    cur.execute("""
        INSERT INTO fullWDC2 (url, id, productID, name, description, price, priceCurrency, brand, manufacturer)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (row['url'], row['id'], row['productID'], row['name'], row['description'], row['price'], row['priceCurrency'], row['brand'], row['manufacturer']))

# Commit the transaction
conn.commit()

# Close the connection
cur.close()
conn.close()
