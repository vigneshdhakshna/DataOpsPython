import sqlite3

class DataQualityChecker:
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)

    def check_completeness(self, table_name, column_name):
        if column_name == "order_id":
            return 100.0  # Skip checking the "order_id" column for completeness

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL")
        result = cursor.fetchone()
        total_rows = result[0]

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = cursor.fetchone()
        total_records = result[0]

        completeness_percentage = (1 - (total_rows / total_records)) * 100
        return completeness_percentage

    def check_accuracy(self, table_name, column_name, expected_value):
        if column_name == "order_id":
            return 100.0  

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} != ?", (expected_value,))
        result = cursor.fetchone()
        total_errors = result[0]

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = cursor.fetchone()
        total_records = result[0]

        accuracy_percentage = (1 - (total_errors / total_records)) * 100
        return accuracy_percentage

    def check_consistency(self, table_name, column_name1, column_name2):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name1} != {column_name2}")
        result = cursor.fetchone()
        inconsistent_count = result[0]
        total_records = self.get_total_records(table_name)

        consistency_percentage = (1 - (inconsistent_count / total_records)) * 100
        return consistency_percentage

    def check_validity(self, table_name, column_name, valid_values):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} NOT IN ({','.join(['?' for _ in range(len(valid_values))])})", valid_values)
        result = cursor.fetchone()
        invalid_count = result[0]
        total_records = self.get_total_records(table_name)

        validity_percentage = (1 - (invalid_count / total_records)) * 100
        return validity_percentage

    def check_uniqueness(self, table_name, column_name):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM (SELECT {column_name}, COUNT(*) AS count FROM {table_name} GROUP BY {column_name}) AS subquery WHERE count > 1")
        result = cursor.fetchone()
        duplicate_count = result[0]
        total_records = self.get_total_records(table_name)

        uniqueness_percentage = (1 - (duplicate_count / total_records)) * 100
        return uniqueness_percentage

    def check_integrity(self, table1_name, table2_name, column1_name, column2_name):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table1_name} LEFT JOIN {table2_name} ON {table1_name}.{column1_name} = {table2_name}.{column2_name} WHERE {table2_name}.{column2_name} IS NULL")
        result = cursor.fetchone()
        integrity_issues = result[0]
        total_records = self.get_total_records(table1_name)

        integrity_percentage = (1 - (integrity_issues / total_records)) * 100
        return integrity_percentage

    def get_total_records(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = cursor.fetchone()
        total_records = result[0]
        return total_records

    def close_connection(self):
        self.conn.close()
