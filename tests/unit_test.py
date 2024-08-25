import unittest
from pyspark.sql import SparkSession
from tokenization_module.Tokenization_v1 import Tokenization

class TestTokenization(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("UnitTest") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_get_spark_session(self):
        args = ["source_data.csv", "schema.json", "lookup.csv"]
        tokenization = Tokenization(args)
        spark = tokenization.get_spark_session()
        self.assertIsNotNone(spark)

    def test_get_schema(self):
        args = ["source_data.csv", "schema.json", "lookup.csv"]
        tokenization = Tokenization(args)
        schema, fields = tokenization.get_schema(self.spark)
        self.assertIsNotNone(schema)
        self.assertIsInstance(fields, list)

    def test_read_file(self):
        args = ["source_data.csv", "schema.json", "lookup.csv"]
        tokenization = Tokenization(args)
        schema, _ = tokenization.get_schema(self.spark)
        df = tokenization.read_file(self.spark, schema)
        self.assertIsNotNone(df)
        self.assertTrue(df.count() > 0)

    def test_read_lookup(self):
        args = ["source_data.csv", "schema.json", "lookup.csv"]
        tokenization = Tokenization(args)
        df = tokenization.read_lookup(self.spark)
        self.assertIsNotNone(df)
        self.assertTrue(df.count() > 0)

    def test_get_pii_tokenize_fields(self):
        args = ["source_data.csv", "schema.json", "lookup.csv"]
        tokenization = Tokenization(args)
        _, fields = tokenization.get_schema(self.spark)
        pii_fields, tokenize_fields = tokenization.get_pii_tokenize_fields(fields)
        self.assertIsInstance(pii_fields, list)
        self.assertIsInstance(tokenize_fields, list)

    def test_mask_pii_field(self):
        args = ["source_data.csv", "schema.json", "lookup.csv"]
        tokenization = Tokenization(args)
        schema, fields = tokenization.get_schema(self.spark)
        df = tokenization.read_file(self.spark, schema)
        pii_fields, _ = tokenization.get_pii_tokenize_fields(fields)
        masked_df = tokenization.mask_pii_field(df, pii_fields)
        self.assertIsNotNone(masked_df)

    def test_tokenize_field(self):
        args = ["source_data.csv", "schema.json", "lookup.csv"]
        tokenization = Tokenization(args)
        schema, fields = tokenization.get_schema(self.spark)
        df = tokenization.read_file(self.spark, schema)
        lookup_df = tokenization.read_lookup(self.spark)
        _, tokenize_fields = tokenization.get_pii_tokenize_fields(fields)
        tokenized_df = tokenization.tokenize_field(df, tokenize_fields, lookup_df)
        self.assertIsNotNone(tokenized_df)

    def test_validate_record_counts(self):
        args = ["source_data.csv", "schema.json", "lookup.csv"]
        tokenization = Tokenization(args)
        schema, fields = tokenization.get_schema(self.spark)
        df = tokenization.read_file(self.spark, schema)
        tokenization.validate_record_counts(self.spark, df, schema)
        # This test will pass if no exception is raised

if __name__ == '__main__':
    unittest.main()