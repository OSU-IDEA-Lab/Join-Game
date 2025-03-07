import csv
import json
import psycopg2
from datetime import datetime

def connect_db(db, db_user, db_port):
    conn = psycopg2.connect(host="/tmp/", database=db, user=db_user, port="db_port)
    return conn

a = 1

# def insert_car_models(db, db_user, db_port, csv_filename):
#     """ Insert car models from a JSON file into the database """
#     conn = connect_db(db, db_user, db_port)
#     cur = conn.cursor()
#     a=1
#     try:
#         with open(csv_filename, 'r') as file:
#             data = json.load(file)  # Load JSON data from file
#             for item in data:
#                 cur.execute("""
#                 INSERT INTO Car_brands (Year, Make, Model, Category)
#                 VALUES (%s, %s, %s, %s)
#                 """, (
#                     item['Year'],
#                     item['Make'],
#                     item['Model'],
#                     item['Category']
#                 ))
#                 print(a)
#                 a+=1
#         conn.commit()
#         print("Data inserted successfully.")
#     except Exception as error:
#         print("Error: ", error)
#         conn.rollback()
#     finally:
#         cur.close()
#         conn.close()

def insert_parking_violations(db, db_user, db_port, csv_filename):
    """ Insert parking violations from a CSV file into the database """
    a = 1
    conn = connect_db(db, db_user, db_port)
    cur = conn.cursor()
    try:
        with open(csv_filename, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Check if the summons number already exists in the database
                cur.execute("SELECT COUNT(*) FROM parking_tickets WHERE summons_number = %s", (row['Summons Number'],))
                exists = cur.fetchone()[0]
                if exists == 0:  # If the summons number is not found, insert the new record
                    cur.execute("""
                    INSERT INTO parking_tickets (
                        summons_number, plate_id, registration_state, plate_type, issue_date, violation_code, vehicle_body_type, vehicle_make, violation_location, violation_precinct, vehicle_color
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row['Summons Number'], row['Plate ID'], row['Registration State'], row['Plate Type'], row['Issue Date'],
                        row['Violation Code'], row['Vehicle Body Type'], row['Vehicle Make'], row['Violation Location'], row['Violation Precinct'], row['Vehicle Color']
                    ))
                    conn.commit()
                    print(a)
                    a+=1
                    if a == 2520407:
                        break
                else:
                    print("Record with summons number {0} already exists.".format(row['Summons Number']))
        print("Data insertion process completed.")
    except Exception as error:
        print("Error: ", error)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def insert_car_accidents(db, db_user, db_port, csv_filename):
    """ Insert parking violations from a CSV file into the database """
    a = 0
    conn = connect_db(db, db_user, db_port)
    cur = conn.cursor()
    try:
        with open(csv_filename, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Check if the summons number already exists in the database
                cur.execute("""
                INSERT INTO car_accidents (
                    Report_Number, Local_Case_Number, Collision_Type, Weather, Person_ID, Vehicle_ID, Vehicle_Make, Vehicle_Model
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['Report Number'], row['Local Case Number'], row['Collision Type'], row['Weather'], row['Person ID'],
                    row['Vehicle ID'], row['Vehicle Make'], row['Vehicle Model']
                ))
                conn.commit()
                print(a)
                a+=1
        print("Data insertion process completed.")
    except Exception as error:
        print("Error: ", error)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python script.py <database> <user> <port> <csv_filename>")
        sys.exit(1)

    # Read command-line arguments
    db = sys.argv[1]
    db_user = sys.argv[2]
    db_port = sys.argv[3]
    csv_filename = sys.argv[4]
    # insert_car_models(db, db_user, db_port, "/data/Car_Model_List.json")
    insert_parking_violations(db, db_user, db_port, "/data/Parking_Violations_Issued_-_Fiscal_Year_2024_20240903.csv")
    # insert_car_accidents(db, db_user, db_port, "/data/Crash_Reporting_-_Drivers_Data.csv")
