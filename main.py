# 📦 Required Libraries
import os
import json
import mysql.connector

def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="phonepe"
    )

def create_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aggregated_transaction (
            state VARCHAR(100), year INT, quarter INT,
            transaction_type VARCHAR(50), transaction_count INT, transaction_amount DOUBLE
        );
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aggregated_user (
            state VARCHAR(100), year INT, quarter INT,
            brand VARCHAR(100), user_count INT, user_percentage DOUBLE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aggregated_insurance (
            state VARCHAR(100), year INT, quarter INT,
            insurance_type VARCHAR(50), insurance_count INT, insurance_amount DOUBLE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS map_user (
            state VARCHAR(100), year INT, quarter INT,
            district VARCHAR(100), registered_users INT, app_opens INT
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS map_map (
            state VARCHAR(100), year INT, quarter INT,
            district VARCHAR(100), transaction_count INT, transaction_amount DOUBLE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS map_insurance (
            state VARCHAR(100), year INT, quarter INT,
            district VARCHAR(100), insurance_count INT, insurance_amount DOUBLE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_user (
            state VARCHAR(100), year INT, quarter INT,
            pincode INT, registered_users INT
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_map (
            state VARCHAR(100), year INT, quarter INT,
            pincode INT, transaction_count INT, transaction_amount DOUBLE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_insurance (
            state VARCHAR(100), year INT, quarter INT,
            pincode INT, insurance_count INT, insurance_amount DOUBLE
        );
    """)

def insert_aggregated_transaction(cursor, base_path):
    path = os.path.join(base_path, "aggregated/transaction/country/india/state")
    for state in os.listdir(path):
        for year in os.listdir(os.path.join(path, state)):
            for file in os.listdir(os.path.join(path, state, year)):
                if file.endswith(".json"):
                    quarter = int(file.replace(".json", ""))
                    with open(os.path.join(path, state, year, file)) as f:
                        data = json.load(f)
                        for item in data.get("data", {}).get("transactionData", []):
                            cursor.execute("""
                                INSERT INTO aggregated_transaction VALUES (%s, %s, %s, %s, %s, %s)
                            """, (
                                state, int(year), quarter,
                                item["name"],
                                item["paymentInstruments"][0]["count"],
                                item["paymentInstruments"][0]["amount"]
                            ))

def insert_aggregated_user(cursor, base_path):
    path = os.path.join(base_path, "aggregated/user/country/india/state")
    for state in os.listdir(path):
        for year in os.listdir(os.path.join(path, state)):
            for file in os.listdir(os.path.join(path, state, year)):
                if file.endswith(".json"):
                    quarter = int(file.replace(".json", ""))
                    with open(os.path.join(path, state, year, file)) as f:
                        data = json.load(f)
                        users_by_device = data.get("data", {}).get("usersByDevice")
                        if users_by_device is not None:
                            for item in users_by_device:
                                cursor.execute("""
                                    INSERT INTO aggregated_user VALUES (%s, %s, %s, %s, %s, %s)
                                """, (
                                    state, int(year), quarter,
                                    item["brand"], item["count"], item["percentage"]
                                ))

def insert_top_user(cursor, base_path):
    path = os.path.join(base_path, "top/user/country/india/state")
    for state in os.listdir(path):
        for year in os.listdir(os.path.join(path, state)):
            for file in os.listdir(os.path.join(path, state, year)):
                if file.endswith(".json"):
                    quarter = int(file.replace(".json", ""))
                    with open(os.path.join(path, state, year, file)) as f:
                        data = json.load(f)
                        for item in data.get("data", {}).get("pincodes", []):
                            cursor.execute("""
                                INSERT INTO top_user VALUES (%s, %s, %s, %s, %s)
                            """, (
                                state, int(year), quarter,
                                item["name"], item["registeredUsers"]
                            ))

# 🏁 Run everything
if __name__ == "__main__":
    base_path = "pulse/data"
    conn = connect_db()
    cursor = conn.cursor()
    create_tables(cursor)

    insert_aggregated_transaction(cursor, base_path)
    insert_aggregated_user(cursor, base_path)
    insert_top_user(cursor, base_path)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ All data loaded into MySQL successfully.")