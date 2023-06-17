from faker import Faker

class DummyDataGenerator:
    def __init__(self, conn):
        self.fake = Faker()
        self.conn = conn

    def generate_customers_data(self, num_rows):
        customers_data = []
        for _ in range(num_rows):
            name = self.fake.name()
            email = self.fake.email()
            address = self.fake.address()
            customers_data.append((None, name, email, address))
        return customers_data

    def generate_products_data(self, num_rows):
        products_data = []
        for _ in range(num_rows):
            name = self.fake.word()
            price = self.fake.random_int(min=10, max=100)
            category = self.fake.word()
            products_data.append((None, name, price, category))
        return products_data

    def generate_orders_data(self, num_rows, num_customers, num_products):
        orders_data = []
        order_ids = set()  # Set to store unique order IDs
        for _ in range(num_rows):
            customer_id = self.fake.random_int(min=1, max=num_customers)
            product_id = self.fake.random_int(min=1, max=num_products)
            quantity = self.fake.random_int(min=1, max=10)
            
            order_id = None
            while order_id is None or order_id in order_ids:
                order_id = self.fake.random_int(min=1, max=num_rows * 2)  # Generate unique order ID
            order_ids.add(order_id)
            
            orders_data.append((order_id, customer_id, product_id, quantity))
        return orders_data
