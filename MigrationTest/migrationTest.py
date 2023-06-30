import sys
sys.path.append(".")

from MigrationTest.data_quality_checker import DataQualityChecker
from MigrationTest.dummy_data_generator import DummyDataGenerator
from MigrationTest.sqlite_connection import SQLiteConnection
from MigrationTest.tinydb_connection import TinyDBConnection
import pandas as pd
from ydata_profiling import ProfileReport



# Number of customers and orders
num_customers = 1000
num_orders = 5000

# Generate dummy data using Faker
customers = DummyDataGenerator.generate_customers(num_customers)
orders = DummyDataGenerator.generate_orders(num_orders, num_customers)

# SQLite Connection
sqlite_conn = SQLiteConnection('./DataBase/sqlite3.db')
sqlite_conn.connect()

# Store customers data in SQLite
DummyDataGenerator.store_customers_sqlite(customers, './DataBase/sqlite3.db')

# Store orders data in SQLite
DummyDataGenerator.store_orders_sqlite(orders, './DataBase/sqlite3.db')

# Close SQLite connection
sqlite_conn.close()

# TinyDB Connection
tinydb_conn = TinyDBConnection('./DataBase/tinydb.json')
tinydb_conn.connect()

# Store customers data in TinyDB
DummyDataGenerator.store_customers_tinydb(customers, './DataBase/tinydb.json')

# Store orders data in TinyDB
DummyDataGenerator.store_orders_tinydb(orders, './DataBase/tinydb.json')

# Close TinyDB connection
tinydb_conn.close()

# Read SQLite data into a DataFrame
sqlite_conn.connect()
query = "SELECT * FROM customers"
customers_df_sqlite = pd.read_sql_query(query, sqlite_conn.conn)

query = "SELECT * FROM orders"
orders_df_sqlite = pd.read_sql_query(query, sqlite_conn.conn)

# Close SQLite connection
sqlite_conn.close()

# Read TinyDB data into a DataFrame
tinydb_conn.connect()
customers_df_tinydb = pd.DataFrame(tinydb_conn.query_all('customers'))
orders_df_tinydb = pd.DataFrame(tinydb_conn.query_all('orders'))

# Close TinyDB connection
tinydb_conn.close()

# Perform data quality checks

# Check if the number of records match in both databases
num_records_check = DataQualityChecker.check_number_of_records(customers_df_sqlite, customers_df_tinydb)
print(num_records_check)

# Perform data integrity check
data_integrity_check = DataQualityChecker.check_data_integrity(customers_df_sqlite, orders_df_sqlite)
print(data_integrity_check)

# Check for duplicate records in customers and orders tables
duplicate_customers_check = DataQualityChecker.check_duplicates(customers_df_sqlite)
print("Duplicate customers check:", duplicate_customers_check)

duplicate_orders_check = DataQualityChecker.check_duplicates(orders_df_sqlite)
print("Duplicate orders check:", duplicate_orders_check)

# Perform data consistency check
data_consistency_check = DataQualityChecker.check_data_consistency(orders_df_sqlite)
print(data_consistency_check)

# Perform Records Matching check
matching_records_check = DataQualityChecker.check_matching_records(customers_df_sqlite,customers_df_tinydb)
print(matching_records_check)

# Generate HTML report using pandas profiling
profile = ProfileReport(customers_df_sqlite)
profile.to_file("./Reports/migration_report.html")
