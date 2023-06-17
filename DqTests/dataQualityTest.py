# Assuming you have already established the SQLite connections for 'customer.db' and 'order.db'
import sys
sys.path.append(".")

import sqlite3
from Utils.data_quality_6dimension import DataQuality6Dimension



db_path = './DataBase/sqlite3.db'
dq = DataQuality6Dimension(db_path)

# Completeness check for 'customer' table
dq.check_completeness('customers')

# Accuracy check for 'email' column in 'customer' table
dq.check_accuracy('customers', 'email')

# Consistency check between 'customer.id' and 'order.customer_id'
dq.check_consistency('customers', 'orders', 'id', 'customer_id')

# Validity check for 'product' column in 'order' table
dq.check_validity('orders', 'product', ['product1', 'product2', 'product3'])

# Uniqueness check for 'id' column in 'customer' table
dq.check_uniqueness('customers', 'id')

# Integrity check between 'customer' and 'order' tables
dq.check_integrity('customers', 'orders', 'id', 'customer_id')
