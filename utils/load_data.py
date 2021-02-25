'''
Creates DB schema for tables: customers, orders
'''

import psycopg2

CUST_TBL = "/Users/dsp/code/school/db-group/Join-Game/data_loading/customer_cl.tbl"
ORDER_TBL = "/Users/dsp/code/school/db-group/Join-Game/data_loading/order_cl.tbl"

conn = psycopg2.connect("host=localhost dbname=tpch user=dsp")
cur = conn.cursor()

create_cust_table = """
    CREATE TABLE IF NOT EXISTS customers(
        id integer PRIMARY KEY,
        uuid text,
        feature1 text,
        feature2 integer,
        feature3 text,
        feature4 float,
        feature5 text,
        feature6 text
    )
"""
cur.execute(create_cust_table)
conn.commit()

with open(CUST_TBL) as in_:
    cur.copy_from(in_, "customers", sep="|")
conn.commit()
print("Created `customers` table and uploaded data.")

create_order_table = """
    CREATE TABLE IF NOT EXISTS orders(
        id integer PRIMARY KEY,
        order_id integer,
        status text,
        value float,
        order_date date,
        priority text,
        handled_by text,
        feature1 integer,
        feature2 text
    )
"""
cur.execute(create_order_table)
conn.commit()

with open(ORDER_TBL) as in_:
    cur.copy_from(in_, "orders", sep="|")
conn.commit()
print("Created `orders` table and uploaded data.")

conn.close()
