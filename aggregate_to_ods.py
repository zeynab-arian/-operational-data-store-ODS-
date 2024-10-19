import pymysql
from pymysql.cursors import DictCursor
import yaml

# Load configuration from YAML
with open('config.yml', 'r') as file:
    config = yaml.safe_load(file)

# Define ODS database connection
ods_config = config['ods']
dbs_config = config['databases']

def create_database_if_not_exists(database_name):
    connection = pymysql.connect(
        host=ods_config["host"],
        port=ods_config["port"],
        user=ods_config["user"],
        password=ods_config["password"],
        cursorclass=DictCursor
    )
    try:
        with connection.cursor() as cursor:
            create_db_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
            cursor.execute(create_db_query)
        connection.commit()
    finally:
        connection.close()

def create_table_if_not_exists(database_name, table_name, columns):
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
            {', '.join(columns)}
        )
    """
    connection = pymysql.connect(
        host=ods_config["host"],
        port=ods_config["port"],
        user=ods_config["user"],
        password=ods_config["password"],
        db=database_name,
        cursorclass=DictCursor
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
        connection.commit()
    finally:
        connection.close()

def insert_into_ods_bulk(data, database_name, table_name, batch_size=1000):
    connection = pymysql.connect(
        host=ods_config["host"],
        port=ods_config["port"],
        user=ods_config["user"],
        password=ods_config["password"],
        db=database_name,
        cursorclass=DictCursor
    )
    try:
        with connection.cursor() as cursor:
            columns = ", ".join(data[0].keys())
            placeholders = ", ".join(["%s"] * len(data[0]))
            sql = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            batch = []
            for row in data:
                batch.append(tuple(row.values()))
                if len(batch) >= batch_size:
                    try:
                        cursor.executemany(sql, batch)
                        connection.commit()
                    except pymysql.MySQLError as e:
                        print(f"Error during batch insert: {e}")
                        # Optionally log to file or take further actions
                    batch = []
            
            # Insert any remaining rows
            if batch:
                try:
                    cursor.executemany(sql, batch)
                    connection.commit()
                except pymysql.MySQLError as e:
                    print(f"Error during final batch insert: {e}")
                    # Optionally log to file or take further actions
    finally:
        connection.close()

def fetch_data(host, port, user, password, database, query):
    connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=database,
        cursorclass=DictCursor
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()
    finally:
        connection.close()

def fetch_table_names(host, port, user, password, database):
    connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=database,
        cursorclass=DictCursor
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            return [row[f"Tables_in_{database}"] for row in cursor.fetchall()]
    finally:
        connection.close()

def aggregate_data_to_ods():
    for db_config in dbs_config:
        for db_name in db_config['databases']:
            print(f"Processing database {db_name} on {db_config['server_id']}...")

            # Create database in ODS if it doesn't exist
            ods_db_name = f"{db_config['server_id']}_{db_name}"
            create_database_if_not_exists(ods_db_name)
            
            # Fetch table names from the source database
            table_names = fetch_table_names(db_config['host'], db_config['port'], db_config['user'], db_config['password'], db_name)
            
            for table_name in table_names:
                query = f"SELECT * FROM {table_name}"
                print(f"Fetching data from {table_name}...")
                data = fetch_data(db_config['host'], db_config['port'], db_config['user'], db_config['password'], db_name, query)
                
                # Prepare the table name for ODS
                ods_table_name = f"{table_name}"
                
                # Fetch columns of the table
                columns_query = f"DESCRIBE {table_name}"
                columns = []
                try:
                    columns_data = fetch_data(db_config['host'], db_config['port'], db_config['user'], db_config['password'], db_name, columns_query)
                    columns = [f"{row['Field']} {row['Type']}" for row in columns_data]
                except Exception as e:
                    print(f"Error describing table {table_name}: {e}")

                # Create the table in ODS if it doesn't exist
                if columns:
                    print(f"Creating table {ods_table_name} in ODS...")
                    create_table_if_not_exists(ods_db_name, ods_table_name, columns)

                    # Insert fetched data into the ODS database
                    if data:
                        print(f"Inserting data into ODS for {table_name}...")
                        insert_into_ods_bulk(data, ods_db_name, ods_table_name, batch_size=1000)

if __name__ == "__main__":
    aggregate_data_to_ods()
    print("Data aggregation complete!")
