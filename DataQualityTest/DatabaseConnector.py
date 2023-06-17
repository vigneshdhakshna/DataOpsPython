import sqlite3

class DatabaseConnector:
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)

    def create_tables(self):
        cursor = self.conn.cursor()

        # Drop existing tables if they exist
        cursor.execute("DROP TABLE IF EXISTS customers")
        cursor.execute("DROP TABLE IF EXISTS orders")
        cursor.execute("DROP TABLE IF EXISTS products")

        # Create customers table
        cursor.execute('''CREATE TABLE customers (
                            customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL,
                            email TEXT NOT NULL,
                            address TEXT
                        )''')

        # Create orders table
        cursor.execute('''CREATE TABLE orders (
                            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            customer_id INTEGER NOT NULL,
                            product_id INTEGER NOT NULL,
                            quantity INTEGER,
                            FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
                            FOREIGN KEY (product_id) REFERENCES products (product_id)
                        )''')

        # Create products table
        cursor.execute('''CREATE TABLE products (
                            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL,
                            price REAL,
                            category TEXT
                        )''')

        self.conn.commit()

    def count_rows_without_match(self, table1_name, table2_name, column1_name, column2_name):
        cursor = self.conn.cursor()
        query = f"SELECT COUNT(*) FROM {table1_name} LEFT JOIN {table2_name} ON {table1_name}.{column1_name} = {table2_name}.{column2_name} WHERE {table2_name}.{column2_name} IS NULL"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        return result

    def insert_data(self, table_name, data):
        cursor = self.conn.cursor()
        placeholders = ', '.join(['?' for _ in range(len(data[0]))])
        cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", data)
        self.conn.commit()

    def close_connection(self):
        self.conn.close()
