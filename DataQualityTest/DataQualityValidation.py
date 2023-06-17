from DataQualityTest.DataQualityChecker import DataQualityChecker


class DataQualityValidation:
    def __init__(self, db_file):
        self.data_quality_checker = DataQualityChecker(db_file)

    def run_data_quality_tests(self):
        completeness = self.data_quality_checker.check_completeness('customers', 'name')
        accuracy = self.data_quality_checker.check_accuracy('products', 'price', 0)
        consistency = self.data_quality_checker.check_consistency('orders', 'quantity', 'product_id')
        validity = self.data_quality_checker.check_validity('customers', 'name', ['John Doe', 'Jane Smith', 'Mike Johnson'])
        uniqueness = self.data_quality_checker.check_uniqueness('orders', 'order_id')
        integrity = self.data_quality_checker.check_integrity('orders','customers', 'customer_id',  'customer_id')

        print(f"Completeness: {completeness}%")
        print(f"Accuracy: {accuracy}%")
        print(f"Consistency: {consistency}%")
        print(f"Validity: {validity}%")
        print(f"Uniqueness: {uniqueness}%")
        print(f"Integrity: {integrity}%")
