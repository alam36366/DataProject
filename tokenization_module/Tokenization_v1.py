import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, LongType
from pyspark.sql.functions import explode, lit, col, broadcast, concat, substring

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Tokenization:
    """
    A class to handle tokenization and masking of data fields.
    """

    def __init__(self, args):
        """
        Initializes the Tokenization class with paths for source data, schema, and lookup.
        """
        self.source_path = args[0]
        self.schema = args[1]
        self.lookup_path = args[2]
        logger.info(f"Initialized Tokenization with source_path={self.source_path}, schema={self.schema}, lookup_path={self.lookup_path}")

    def get_spark_session(self):
        """
        Creates and returns a SparkSession.
        """
        logger.info("Creating Spark session.")
        spark = SparkSession.builder.appName("tokenization").getOrCreate()
        logger.info("Spark session created.")
        return spark

    def get_schema(self, spark):
        """
        Reads and returns the schema from a JSON file.
        """
        logger.info("Reading schema from JSON.")
        try:
            schema_df = spark.read.option("multiline", "true").json(self.schema)
            schema_df = schema_df.withColumn('field_exp', explode(schema_df.fields))
            schema_df = schema_df.select('field_exp.*')
            fields = schema_df.rdd.map(lambda row: row.asDict()).collect()
            
            struct_fields = []
            for field in fields:
                field_name = field['name']
                field_type = field['type']
                nullable = field['nullable']
                spark_type = self._get_spark_type(field_type)
                struct_fields.append(StructField(field_name, spark_type, nullable))
            
            logger.info("Schema obtained successfully.")
            return StructType(struct_fields), fields
        except Exception as e:
            logger.error(f"Error reading schema: {e}")
            raise

    def _get_spark_type(self, field_type):
        """
        Maps field type from schema to Spark SQL data type.
        """
        if field_type == 'integer':
            return IntegerType()
        elif field_type == 'string':
            return StringType()
        elif field_type == 'date':
            return DateType()
        elif field_type == 'long':
            return LongType()
        else:
            raise ValueError(f"Unsupported field type: {field_type}")

    def read_file(self, spark, schema):
        """
        Reads data from a CSV file into a DataFrame using the provided schema.
        """
        logger.info(f"Reading file from {self.source_path}.")
        try:
            df = spark.read.csv(self.source_path, schema=schema, header=True)
            logger.info("File read successfully.")
            return df
        except Exception as e:
            logger.error(f"Error reading file: {e}")
            raise

    def read_lookup(self, spark):
        """
        Reads lookup data from a CSV file into a DataFrame.
        """
        logger.info(f"Reading lookup file from {self.lookup_path}.")
        try:
            lookup_df = spark.read.csv(self.lookup_path, inferSchema=True, header=True)
            logger.info("Lookup file read successfully.")
            return lookup_df
        except Exception as e:
            logger.error(f"Error reading lookup file: {e}")
            raise

    def get_pii_tokenize_fields(self, fields):
        """
        Determines which fields are PII and which should be tokenized.
        """
        logger.info("Determining PII and tokenize fields.")
        pii_fields = [field['name'] for field in fields if field['pii']]
        tokenize_fields = [field['name'] for field in fields if field['tokenize']]
        logger.info(f"PII fields: {pii_fields}")
        logger.info(f"Tokenize fields: {tokenize_fields}")
        return pii_fields, tokenize_fields

    def mask_pii_field(self, data, pii_fields):
        """
        Masks PII fields in the DataFrame by setting their values to None.
        """
        logger.info("Masking PII fields.")
        masked_data = data.select(*[lit(None).alias(c) if c in pii_fields else col(c) for c in data.columns])
        logger.info("PII fields masked.")
        return masked_data

    def tokenize_field(self, masked_data, tokenize_fields, lookup):
        """
        Tokenizes fields in the DataFrame based on the lookup data.
        """
        logger.info("Tokenizing fields.")
        try:
            lookup_joined = masked_data.join(broadcast(lookup), masked_data["employee_id"] == lookup["lookup_emp_id"])
            lookup_col = "account_num"
            # display(masked_data)
            lookup_joined = lookup_joined.select(*[col(c) for c in masked_data.columns], lookup[lookup_col])
            for field in tokenize_fields:
                lookup_joined = lookup_joined.withColumn(
                    field, concat(substring(col(field), 1, 2), col(lookup_col), substring(col(field), 15, 2))
                )
            logger.info("Fields tokenized.")
            tokenized_data = lookup_joined.drop(lookup_col)
            return tokenized_data
        except Exception as e:
            logger.error(f"Error during tokenization: {e}")
            raise

    def validate_record_counts(self, spark, final_data: DataFrame, schema: StructType) -> None:
        """
        Validates that no records are dropped during data processing by comparing
        the record count of raw data with the final processed data.
        """
        try:
            # Read the raw data
            logger.info(f"Reading raw data from {self.source_path}")
            raw_data = spark.read.csv(self.source_path, schema=schema, header=True)
            
            # Count records in raw data
            raw_data_count = raw_data.count()
            logger.info(f"Record count in raw data: {raw_data_count}")
            
            # Count records in final data
            final_data_count = final_data.count()
            logger.info(f"Record count in final data: {final_data_count}")
            
            # Validation: Ensure no records are dropped
            if raw_data_count != final_data_count:
                logger.warning("Record count mismatch! Some records may have been dropped during processing.")
            else:
                logger.info("Validation successful: No records were dropped during processing.")
        
        except Exception as e:
            logger.error(f"Error during validation: {e}")
            raise


def main(args):
    """
    Main function to execute the tokenization process.
    """
    logger.info("Starting Job.")
    token_object = Tokenization(args)
    spark = token_object.get_spark_session()
    schema, fields = token_object.get_schema(spark)
    data = token_object.read_file(spark, schema)
    lookup = token_object.read_lookup(spark)
    pii_fields, tokenize_fields = token_object.get_pii_tokenize_fields(fields)
    masked_data = token_object.mask_pii_field(data, pii_fields)
    tokenized_data = token_object.tokenize_field(masked_data, tokenize_fields, lookup)
    token_object.validate_record_counts(spark, tokenized_data, schema)
    logger.info("Final Data......... \n")
    tokenized_data.show(20, False)
    logger.info("Job completed!")

if __name__ == "__main__":
    main(sys.argv[1:])
