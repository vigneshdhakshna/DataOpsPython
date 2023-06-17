import sqlite3

class DataQuality6Dimension:
    def __init__(self, db_path):
        self.db_path = db_path

    def check_completeness(self, table_name):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]

        print(f"Completeness check for table '{table_name}':")
        print(f"Total rows: {total_rows}")

    def check_accuracy(self, table_name, column_name):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL")
            null_count = cursor.fetchone()[0]

        print(f"Accuracy check for column '{column_name}' in table '{table_name}':")
        print(f"Null values count: {null_count}")

    def check_consistency(self, table1_name, table2_name, column1_name, column2_name):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table1_name} WHERE {column1_name} NOT IN (SELECT {column2_name} FROM {table2_name})")
            inconsistent_count = cursor.fetchone()[0]

        print(f"Consistency check between '{table1_name}.{column1_name}' and '{table2_name}.{column2_name}':")
        print(f"Inconsistent values count: {inconsistent_count}")

    def check_validity(self, table_name, column_name, valid_values):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            valid_values_str = ", ".join([f"'{value}'" for value in valid_values])
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} NOT IN ({valid_values_str})")
            invalid_count = cursor.fetchone()[0]

        print(f"Validity check for column '{column_name}' in table '{table_name}':")
        print(f"Invalid values count: {invalid_count}")

    def check_uniqueness(self, table_name, column_name):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IN (SELECT {column_name} FROM {table_name} GROUP BY {column_name} HAVING COUNT(*) > 1)")
            duplicate_count = cursor.fetchone()[0]

        print(f"Uniqueness check for column '{column_name}' in table '{table_name}':")
        print(f"Duplicate values count: {duplicate_count}")

    def check_integrity(self, table1_name, table2_name, column1_name, column2_name):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table1_name} LEFT JOIN {table2_name} ON {table1_name}.{column1_name} = {table2_name}.{column2_name} WHERE {table2_name}.{column2_name} IS NULL")
            missing_count = cursor.fetchone()[0]

        print(f"Integrity check between '{table1_name}' and '{table2_name}':")
        print(f"Missing values count: {missing_count}")
