
class DataQualityChecker:
    @staticmethod
    def check_number_of_records(dataframe1, dataframe2):
        num_records_df1 = len(dataframe1)
        num_records_df2 = len(dataframe2)

        if num_records_df1 == num_records_df2:
            return "Number of records match."
        else:
            return "Number of records do not match."

    @staticmethod
    def check_data_integrity(customers_df, orders_df):
        invalid_orders = orders_df[~orders_df['customer_id'].isin(customers_df['id'])]

        if invalid_orders.empty:
            return "Data integrity check passed. All customer IDs in orders table exist in the customers table."
        else:
            return "Data integrity check failed. Invalid customer IDs found in the orders table."

    @staticmethod
    def check_duplicates(dataframe):
        duplicate_records = dataframe[dataframe.duplicated()]

        if duplicate_records.empty:
            return "No duplicate records found."
        else:
            return "Duplicate records found."

    @staticmethod
    def check_data_consistency(orders_df):
        invalid_quantity = orders_df[orders_df['quantity'] <= 0]
        invalid_price = orders_df[orders_df['price'] <= 0]

        if invalid_quantity.empty and invalid_price.empty:
            return "Data consistency check passed. Quantity and price values are valid."
        else:
            error_message = "Data consistency check failed. "
            if not invalid_quantity.empty:
                error_message += "Invalid quantity values. "
            if not invalid_price.empty:
                error_message += "Invalid price values."
            return error_message
