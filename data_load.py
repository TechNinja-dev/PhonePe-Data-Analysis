# 📦 Required Libraries
import os
import json
import mysql.connector
from tqdm import tqdm

def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="phonepe"
    )
        # 'aggregated_transaction'  'aggregated_user'  'aggregated_insurance'
        # 'map_user'  'map_map'  'map_insurance'
        # 'top_user'  'top_map'  'top_insurance'

def create_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aggregated_transaction (
            state VARCHAR(100), year INT, quarter INT,
            transaction_type VARCHAR(50), transaction_count BIGINT, transaction_amount DOUBLE
        );
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aggregated_user (
            state VARCHAR(100), year INT, quarter INT,
            brand VARCHAR(100), user_count BIGINT, user_percentage DOUBLE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aggregated_insurance (
            state VARCHAR(100), year INT, quarter INT,
            insurance_type VARCHAR(50), insurance_count BIGINT, insurance_amount DOUBLE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS map_user (
            state VARCHAR(100), year INT, quarter INT,
            district VARCHAR(100), registered_users BIGINT, app_opens BIGINT
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS map_map (
            state VARCHAR(100), year INT, quarter INT,
            district VARCHAR(100), transaction_count BIGINT, transaction_amount DOUBLE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS map_insurance (
            state VARCHAR(100), year INT, quarter INT,
            district VARCHAR(100), insurance_count BIGINT, insurance_amount DOUBLE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_user (
            state VARCHAR(100), year INT, quarter INT,
            pincode INT, registered_users BIGINT
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_map (
            state VARCHAR(100), year INT, quarter INT,
            location_type ENUM('pincode', 'district'),
            location_name VARCHAR(100),
            transaction_count BIGINT, 
            transaction_amount DOUBLE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_insurance (
            state VARCHAR(100), year INT, quarter INT,
            location_type ENUM('pincode', 'district'),
            location_name VARCHAR(100),
            insurance_count BIGINT, 
            insurance_amount DOUBLE
        );
    """)

def insert_aggregated_transaction(cursor, base_path):
    path = os.path.join(base_path, "aggregated/transaction/country/india/state")
    batch = []
    batch_size = 1000
    
    states = os.listdir(path)
    for state in tqdm(states, desc="Processing Transaction States"):
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
            
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
                
            for file in os.listdir(year_path):
                if file.endswith(".json"):
                    quarter = int(file.replace(".json", ""))
                    file_path = os.path.join(year_path, file)
                    
                    try:
                        with open(file_path) as f:
                            data = json.load(f)
                            for item in data.get("data", {}).get("transactionData", []):
                                batch.append((
                                    state, int(year), quarter,
                                    item["name"],
                                    item["paymentInstruments"][0]["count"],
                                    item["paymentInstruments"][0]["amount"]
                                ))
                                
                                if len(batch) >= batch_size:
                                    cursor.executemany("""
                                        INSERT INTO aggregated_transaction VALUES (%s, %s, %s, %s, %s, %s)
                                    """, batch)
                                    batch = []
                    except Exception as e:
                        print(f"Error processing {file_path}: {str(e)}")
                        continue
    
    if batch:
        cursor.executemany("""
            INSERT INTO aggregated_transaction VALUES (%s, %s, %s, %s, %s, %s)
        """, batch)

def insert_aggregated_user(cursor, base_path):
    path = os.path.join(base_path, "aggregated/user/country/india/state")
    batch = []
    batch_size = 1000
    
    states = os.listdir(path)
    for state in tqdm(states, desc="Processing User States"):
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
            
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
                
            for file in os.listdir(year_path):
                if file.endswith(".json"):
                    quarter = int(file.replace(".json", ""))
                    file_path = os.path.join(year_path, file)
                    
                    try:
                        with open(file_path) as f:
                            data = json.load(f)
                            users_by_device = data.get("data", {}).get("usersByDevice")
                            if users_by_device is not None:
                                for item in users_by_device:
                                    batch.append((
                                        state, int(year), quarter,
                                        item["brand"], item["count"], item["percentage"]
                                    ))
                                    
                                    if len(batch) >= batch_size:
                                        cursor.executemany("""
                                            INSERT INTO aggregated_user VALUES (%s, %s, %s, %s, %s, %s)
                                        """, batch)
                                        batch = []
                    except Exception as e:
                        print(f"Error processing {file_path}: {str(e)}")
                        continue
    
    if batch:
        cursor.executemany("""
            INSERT INTO aggregated_user VALUES (%s, %s, %s, %s, %s, %s)
        """, batch)

def insert_top_user(cursor, base_path):
    path = os.path.join(base_path, "top/user/country/india/state")
    batch = []
    batch_size = 1000
    
    states = os.listdir(path)
    for state in tqdm(states, desc="Processing Top User States"):
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
            
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
                
            for file in os.listdir(year_path):
                if file.endswith(".json"):
                    quarter = int(file.replace(".json", ""))
                    file_path = os.path.join(year_path, file)
                    
                    try:
                        with open(file_path) as f:
                            data = json.load(f)
                            for item in data.get("data", {}).get("pincodes", []):
                                batch.append((
                                    state, int(year), quarter,
                                    item["name"], item["registeredUsers"]
                                ))
                                
                                if len(batch) >= batch_size:
                                    cursor.executemany("""
                                        INSERT INTO top_user VALUES (%s, %s, %s, %s, %s)
                                    """, batch)
                                    batch = []
                    except Exception as e:
                        print(f"Error processing {file_path}: {str(e)}")
                        continue
    
    if batch:
        cursor.executemany("""
            INSERT INTO top_user VALUES (%s, %s, %s, %s, %s)
        """, batch)

def insert_aggregated_insurance(cursor, base_path):
    path = os.path.join(base_path, "aggregated/insurance/country/india/state")
    batch = []
    batch_size = 1000
    
    states = os.listdir(path)
    for state in tqdm(states, desc="Processing Insurance States"):
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
            
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
                
            for file in os.listdir(year_path):
                if file.endswith(".json"):
                    quarter = int(file.replace(".json", ""))
                    file_path = os.path.join(year_path, file)
                    
                    try:
                        with open(file_path) as f:
                            data = json.load(f)
                            for item in data.get("data", {}).get("transactionData", []):
                                batch.append((
                                    state, int(year), quarter,
                                    item["name"],
                                    item["paymentInstruments"][0]["count"],
                                    item["paymentInstruments"][0]["amount"]
                                ))
                                
                                if len(batch) >= batch_size:
                                    cursor.executemany("""
                                        INSERT INTO aggregated_insurance VALUES (%s, %s, %s, %s, %s, %s)
                                    """, batch)
                                    batch = []
                    except Exception as e:
                        print(f"Error processing {file_path}: {str(e)}")
                        continue
    
    if batch:
        cursor.executemany("""
            INSERT INTO aggregated_insurance VALUES (%s, %s, %s, %s, %s, %s)
        """, batch)

def insert_map_transaction(cursor, base_path):
    path = os.path.join(base_path, "map/transaction/hover/country/india/state")
    batch = []
    batch_size = 1000
    
    states = os.listdir(path)
    for state in tqdm(states, desc="Processing Map Transaction States"):
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
            
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
                
            for file in os.listdir(year_path):
                if file.endswith(".json"):
                    quarter = int(file.replace(".json", ""))
                    file_path = os.path.join(year_path, file)
                    
                    try:
                        with open(file_path) as f:
                            data = json.load(f)
                            hover_data_list = data.get("data", {}).get("hoverDataList", [])
                            
                            for item in hover_data_list:
                                district = item.get("name")
                                metrics = item.get("metric", [])
                                
                                if metrics and len(metrics) > 0:
                                    batch.append((
                                        state, int(year), quarter,
                                        district,
                                        metrics[0].get("count", 0),
                                        metrics[0].get("amount", 0.0)
                                    ))
                                    
                                    if len(batch) >= batch_size:
                                        cursor.executemany("""
                                            INSERT INTO map_map VALUES (%s, %s, %s, %s, %s, %s)
                                        """, batch)
                                        batch = []
                    except Exception as e:
                        print(f"Error processing {file_path}: {str(e)}")
                        continue
    
    if batch:
        cursor.executemany("""
            INSERT INTO map_map VALUES (%s, %s, %s, %s, %s, %s)
        """, batch)

def insert_map_user(cursor, base_path):
    path = os.path.join(base_path, "map/user/hover/country/india/state")
    batch = []
    batch_size = 1000
    
    states = os.listdir(path)
    for state in tqdm(states, desc="Processing Map User States"):
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
            
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
                
            for file in os.listdir(year_path):
                if file.endswith(".json"):
                    quarter = int(file.replace(".json", ""))
                    file_path = os.path.join(year_path, file)
                    
                    try:
                        with open(file_path) as f:
                            data = json.load(f)
                            for district, values in data.get("data", {}).get("hoverData", {}).items():
                                batch.append((
                                    state, int(year), quarter,
                                    district,
                                    values["registeredUsers"],
                                    values["appOpens"]
                                ))
                                
                                if len(batch) >= batch_size:
                                    cursor.executemany("""
                                        INSERT INTO map_user VALUES (%s, %s, %s, %s, %s, %s)
                                    """, batch)
                                    batch = []
                    except Exception as e:
                        print(f"Error processing {file_path}: {str(e)}")
                        continue
    
    if batch:
        cursor.executemany("""
            INSERT INTO map_user VALUES (%s, %s, %s, %s, %s, %s)
        """, batch)

def insert_map_insurance(cursor, base_path):
    path = os.path.join(base_path, "map/insurance/hover/country/india/state")
    batch = []
    batch_size = 1000
    
    states = os.listdir(path)
    for state in tqdm(states, desc="Processing Map Insurance States"):
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
            
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
                
            for file in os.listdir(year_path):
                if file.endswith(".json"):
                    quarter = int(file.replace(".json", ""))
                    file_path = os.path.join(year_path, file)
                    
                    try:
                        with open(file_path) as f:
                            data = json.load(f)
                            hover_data_list = data.get("data", {}).get("hoverDataList", [])
                            
                            for item in hover_data_list:
                                district = item.get("name")
                                metrics = item.get("metric", [])
                                
                                if metrics and len(metrics) > 0:
                                    batch.append((
                                        state, int(year), quarter,
                                        district,
                                        metrics[0].get("count", 0),
                                        metrics[0].get("amount", 0.0)
                                    ))
                                    
                                    if len(batch) >= batch_size:
                                        cursor.executemany("""
                                            INSERT INTO map_insurance VALUES (%s, %s, %s, %s, %s, %s)
                                        """, batch)
                                        batch = []
                    except Exception as e:
                        print(f"Error processing {file_path}: {str(e)}")
                        continue
    
    if batch:
        cursor.executemany("""
            INSERT INTO map_insurance VALUES (%s, %s, %s, %s, %s, %s)
        """, batch)

def insert_top_transaction(cursor, base_path):
    path = os.path.join(base_path, "top/transaction/country/india/state")
    batch = []
    batch_size = 1000
    
    states = os.listdir(path)
    for state in tqdm(states, desc="Processing Top Transaction States"):
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
            
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
                
            for file in os.listdir(year_path):
                if file.endswith(".json"):
                    quarter = int(file.replace(".json", ""))
                    file_path = os.path.join(year_path, file)
                    
                    try:
                        with open(file_path) as f:
                            data = json.load(f)
                            districts = data.get("data", {}).get("districts", [])
                            pincodes = data.get("data", {}).get("pincodes", [])
                            
                            # Process districts data
                            for item in districts:
                                entity_name = item.get("entityName")
                                metric = item.get("metric", {})
                                batch.append((
                                    state, int(year), quarter,
                                    'district',  # location_type
                                    entity_name,  # location_name
                                    metric.get("count", 0),
                                    metric.get("amount", 0.0)
                                ))
                            
                            # Process pincodes data
                            for item in pincodes:
                                entity_name = item.get("entityName")
                                metric = item.get("metric", {})
                                
                                # Check if entity_name is numeric pincode or district name
                                loc_type = 'pincode' if entity_name.isdigit() else 'district'
                                batch.append((
                                    state, int(year), quarter,
                                    loc_type,
                                    entity_name,
                                    metric.get("count", 0),
                                    metric.get("amount", 0.0)
                                ))
                                
                                if len(batch) >= batch_size:
                                    cursor.executemany("""
                                        INSERT INTO top_map 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    """, batch)
                                    batch = []
                    except Exception as e:
                        print(f"Error processing {file_path}: {str(e)}")
                        continue
    
    if batch:
        cursor.executemany("""
            INSERT INTO top_map VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, batch)

def insert_top_insurance(cursor, base_path):
    path = os.path.join(base_path, "top/insurance/country/india/state")
    batch = []
    batch_size = 1000
    
    states = os.listdir(path)
    for state in tqdm(states, desc="Processing Top Insurance States"):
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
            
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
                
            for file in os.listdir(year_path):
                if file.endswith(".json"):
                    quarter = int(file.replace(".json", ""))
                    file_path = os.path.join(year_path, file)
                    
                    try:
                        with open(file_path) as f:
                            data = json.load(f)
                            districts = data.get("data", {}).get("districts", [])
                            pincodes = data.get("data", {}).get("pincodes", [])
                            
                            # Process districts data
                            for item in districts:
                                entity_name = item.get("entityName")
                                metric = item.get("metric", {})
                                batch.append((
                                    state, int(year), quarter,
                                    'district',  # location_type
                                    entity_name,  # location_name
                                    metric.get("count", 0),
                                    metric.get("amount", 0.0)
                                ))
                            
                            # Process pincodes data
                            for item in pincodes:
                                entity_name = item.get("entityName")
                                metric = item.get("metric", {})
                                
                                # Check if entity_name is numeric pincode or district name
                                loc_type = 'pincode' if entity_name.isdigit() else 'district'
                                batch.append((
                                    state, int(year), quarter,
                                    loc_type,
                                    entity_name,
                                    metric.get("count", 0),
                                    metric.get("amount", 0.0)
                                ))
                                
                                if len(batch) >= batch_size:
                                    cursor.executemany("""
                                        INSERT INTO top_insurance 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    """, batch)
                                    batch = []
                    except Exception as e:
                        print(f"Error processing {file_path}: {str(e)}")
                        continue
    
    if batch:
        cursor.executemany("""
            INSERT INTO top_insurance VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, batch)

base_path = "pulse/data"
conn = connect_db()
cursor = conn.cursor()

        
print("🛠️ Creating tables with updated schema...")
create_tables(cursor)
        

print("⏳ Loading aggregated transaction data...")
insert_aggregated_transaction(cursor, base_path)
        
print("⏳ Loading aggregated user data...")
insert_aggregated_user(cursor, base_path)
        
print("⏳ Loading aggregated insurance data...")
insert_aggregated_insurance(cursor, base_path)
        
print("⏳ Loading map transaction data...")
insert_map_transaction(cursor, base_path)
        
print("⏳ Loading map user data...")
insert_map_user(cursor, base_path)
        
print("⏳ Loading map insurance data...")
insert_map_insurance(cursor, base_path)
        
print("⏳ Loading top transaction data...")
insert_top_transaction(cursor, base_path)
        
print("⏳ Loading top user data...")
insert_top_user(cursor, base_path)
        
print("⏳ Loading top insurance data...")
insert_top_insurance(cursor, base_path)
        
conn.commit()
print("✅ All data loaded into MySQL successfully.")
cursor.close()
conn.close()
