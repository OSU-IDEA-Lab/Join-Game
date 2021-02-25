'''
Creates DB schema for tables: customers, orders
And then loads them into a PostgreSQL db
'''

import os
import argparse
try:
    import psycopg2
except ModuleNotFoundError:
    os.system("pip3 install psycopg2-binary")
    import psycopg2

parser = argparse.ArgumentParser(description="Data loader")
parser.add_argument("--cust", help="Customer table path")
parser.add_argument("--order", help="Order table path")
parser.add_argument("--db_host", default="localhost", help="DB host addr")
parser.add_argument("--db_port", default="5009", help="DB port number")
parser.add_argument("--db_name", default="tpch", help="DB name")
parser.add_argument("--db_user", default="dsp", help="DB username")

cfg = parser.parse_args()

connection_str = "host={host} port={port} dbname={dbname} user={uname}".format(
    host=cfg.db_host, port=cfg.db_port, dbname=cfg.db_name, uname=cfg.db_user
)
conn = psycopg2.connect(connection_str)
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

with open(cfg.cust) as in_:
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

with open(cfg.order) as in_:
    cur.copy_from(in_, "orders", sep="|")
conn.commit()
print("Created `orders` table and uploaded data.")

conn.close()
