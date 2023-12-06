import unittest
from pyspark.sql import SparkSession
from SparkRepo.src.Assignment_1.utils import *

class TestMySparkFunctions(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("Test_Question_1").getOrCreate()

        # Generate example dataframes for testing
        self.df_users = self.spark.createDataFrame([
            (1, 'Ashwath', 'India'),
            (2, 'Raj', 'USA'),
            # Add more test data if needed
        ], ['user_id', 'username', 'location'])

        self.df_transactions = self.spark.createDataFrame([
            (1, 101, 1, 50),
            (1, 102, 2, 30),
            (2, 103, 1, 20),
            # Add more test data if needed
        ], ['userid', 'transaction_id', 'product_id', 'price'])

    def tearDown(self):
        self.spark.stop()

    def test_unique_location_count(self):
        result = unique_location_count(self.df_users, self.df_transactions, self.spark)
        # Add assertions based on expected schema or content of the result DataFrame
        self.assertTrue("product_id" in result.columns)
        self.assertTrue("location" in result.columns)
        self.assertTrue("count_unique_locations" in result.columns)
        # You can add more specific assertions to validate the content of the result DataFrame

    def test_products_bought(self):
        result = products_bought(self.df_users, self.df_transactions, self.spark)
        # Add assertions based on expected schema or content of the result DataFrame
        self.assertTrue("userid" in result.columns)
        self.assertTrue("products" in result.columns)
        # You can add more specific assertions to validate the content of the result DataFrame

    def test_total_spending(self):
        result = total_spending(self.df_users, self.df_transactions, self.spark)
        # Add assertions based on expected schema or content of the result DataFrame
        self.assertTrue("user_id" in result.columns)
        self.assertTrue("product_id" in result.columns)
        self.assertTrue("Total_Spendings" in result.columns)
        # You can add more specific assertions to validate the content of the result DataFrame

if __name__ == '__main__':
    unittest.main()
