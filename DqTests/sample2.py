from faker import Faker
import random
import sqlite3
from tinydb import TinyDB, Query
import pandas as pd
from ydata_profiling import ProfileReport


# Perform data quality checks and migration testing

# Read SQLite data into a DataFrame
conn = sqlite3.connect('./DataBase/sqlite3.db')
query = "SELECT * FROM customers"
customers_df_sqlite = pd.read_sql_query(query, conn)

query = "SELECT * FROM orders"
orders_df_sqlite = pd.read_sql_query(query, conn)

# Read TinyDB data into a DataFrame
db = TinyDB('./DataBase/tinydb.json')
customer_table = db.table('customers')
customers_df_tinydb = pd.DataFrame(customer_table.all())

order_table = db.table('orders')
orders_df_tinydb = pd.DataFrame(order_table.all())

# Check if the number of records match in both databases
num_records_sqlite = len(customers_df_sqlite)
num_records_tinydb = len(customers_df_tinydb)

if num_records_sqlite == num_records_tinydb:
    print("Number of records match in SQLite and TinyDB.")
else:
    print("Number of records do not match in SQLite and TinyDB.")

# Perform data integrity check
invalid_orders = orders_df_sqlite[~orders_df_sqlite['customer_id'].isin(customers_df_sqlite['id'])]

if invalid_orders.empty:
    print("Data integrity check passed. All customer IDs in orders table exist in the customers table.")
else:
    print("Data integrity check failed. Invalid customer IDs found in the orders table:")
    print(invalid_orders)

# Check for duplicate records in customers table
duplicate_customers = customers_df_sqlite[customers_df_sqlite.duplicated()]
if duplicate_customers.empty:
    print("No duplicate records found in the customers table.")
else:
    print("Duplicate records found in the customers table:")
    print(duplicate_customers)

# Check for duplicate records in orders table
duplicate_orders = orders_df_sqlite[orders_df_sqlite.duplicated()]
if duplicate_orders.empty:
    print("No duplicate records found in the orders table.")
else:
    print("Duplicate records found in the orders table:")
    print(duplicate_orders)

# Perform data consistency check
invalid_quantity = orders_df_sqlite[orders_df_sqlite['quantity'] <= 0]
invalid_price = orders_df_sqlite[orders_df_sqlite['price'] <= 0]

if invalid_quantity.empty and invalid_price.empty:
    print("Data consistency check passed. Quantity and price values are valid.")
else:
    print("Data consistency check failed. Invalid quantity and/or price values found in the orders table:")
    if not invalid_quantity.empty:
        print("Invalid quantity values:")
        print(invalid_quantity)
    if not invalid_price.empty:
        print("Invalid price values:")
        print(invalid_price)

# Compare sample records from SQLite and TinyDB
sample_records_sqlite = customers_df_sqlite.sample(n=5)
sample_records_tinydb = customers_df_tinydb.sample(n=5)

print(sample_records_sqlite.columns)
print(sample_records_tinydb.columns)

matching_records = sample_records_sqlite.merge(sample_records_tinydb, left_on='name', right_on='name')

if matching_records.empty:
    print("Migration testing passed. Sample records from SQLite and TinyDB match.")
else:
    print("Migration testing failed. Inconsistent sample records found between SQLite and TinyDB:")
    print(matching_records)

# Generate HTML report using pandas profiling
profile = ProfileReport(customers_df_sqlite)
profile.to_file("customer_report.html")
