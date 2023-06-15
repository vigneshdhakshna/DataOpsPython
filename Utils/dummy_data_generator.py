from faker import Faker
import random
import sqlite3
from tinydb import TinyDB

class DummyDataGenerator:
    @staticmethod
    def generate_customers(num_customers):
        fake = Faker()
        customers = []
        for _ in range(num_customers):
            customer = {
                'name': fake.name(),
                'email': fake.email(),
                'address': fake.address()
            }
            customers.append(customer)
        return customers

    @staticmethod
    def generate_orders(num_orders, num_customers):
        fake = Faker()
        orders = []
        for _ in range(num_orders):
            order = {
                'order_id': fake.random_number(digits=5),
                'customer_id': random.choice(range(num_customers)),
                'product': fake.random_element(elements=('Product A', 'Product B', 'Product C')),
                'quantity': fake.random_int(min=1, max=10),
                'price': fake.random_int(min=10, max=100)
            }
            orders.append(order)
        return orders

    @staticmethod
    def store_customers_sqlite(customers, db_file):
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS customers
                          (id INTEGER PRIMARY KEY,
                           name TEXT,
                           email TEXT,
                           address TEXT)''')
        for customer in customers:
            cursor.execute("INSERT INTO customers (name, email, address) VALUES (?, ?, ?)",
                           (customer['name'], customer['email'], customer['address']))
        conn.commit()
        conn.close()

    @staticmethod
    def store_orders_sqlite(orders, db_file):
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS orders
                          (id INTEGER PRIMARY KEY,
                           order_id INTEGER,
                           customer_id INTEGER,
                           product TEXT,
                           quantity INTEGER,
                           price INTEGER)''')
        for order in orders:
            cursor.execute("INSERT INTO orders (order_id, customer_id, product, quantity, price) VALUES (?, ?, ?, ?, ?)",
                           (order['order_id'], order['customer_id'], order['product'], order['quantity'], order['price']))
        conn.commit()
        conn.close()

    @staticmethod
    def store_customers_tinydb(customers, db_file):
        db = TinyDB(db_file)
        customer_table = db.table('customers')
        customer_table.insert_multiple(customers)

    @staticmethod
    def store_orders_tinydb(orders, db_file):
        db = TinyDB(db_file)
        order_table = db.table('orders')
        order_table.insert_multiple(orders)
