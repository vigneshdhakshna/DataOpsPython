import sys
sys.path.append(".")

from DataQualityTest.DatabaseConnector import DatabaseConnector
from DataQualityTest.DummyDataGenerator import DummyDataGenerator
from DataQualityTest.DataQualityValidation import DataQualityValidation

# Configuration
db_file = "./DataBase/ces.db"

# Create database connector
db_connector = DatabaseConnector(db_file)
db_connector.create_tables()

# Generate dummy data
data_generator = DummyDataGenerator(db_connector.conn)
customers_data = data_generator.generate_customers_data(5000)
products_data = data_generator.generate_products_data(2000)
orders_data = data_generator.generate_orders_data(10000, 5000, 2000)

# Insert data into tables
db_connector.insert_data("customers", customers_data)
db_connector.insert_data("products", products_data)
db_connector.insert_data("orders", orders_data)

# Close database connection
db_connector.close_connection()

# Run data quality tests
data_quality_validation = DataQualityValidation(db_file)
data_quality_validation.run_data_quality_tests()