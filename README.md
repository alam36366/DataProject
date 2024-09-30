# Encryption Process

This project contains a Python script for tokenizing and masking data fields using Apache Spark. The script processes data from CSV files, applies tokenization and masking based on provided schemas, and validates the data to ensure no records are dropped during processing.

## Overview

The script performs the following steps:

1. **Initialize**: Set up paths for source data, schema, and lookup files.
2. **Create Spark Session**: Establish a Spark session.
3. **Read Schema**: Load the schema from a JSON file.
4. **Read Data**: Load source and lookup data from CSV files.
5. **Determine PII and Tokenize Fields**: Identify fields that are Personally Identifiable Information (PII) and those that need tokenization.
6. **Mask PII Fields**: Mask the PII fields by setting their values to `None`.
7. **Tokenize Fields**: Apply tokenization to specified fields based on lookup data.
8. **Validate Record Counts**: Ensure no records are dropped by comparing the record counts of raw and processed data.

## Requirements

- Apache Spark (version 3.0.0 or later recommended)
- Python 3.x
- `pyspark` library

You can install `pyspark` using pip:

pip install pyspark

## Usage
To run the script, provide the paths to the source data, schema, and lookup files as command-line arguments:
python script.py <source_path> <schema_path> <lookup_path>
**Example:**
python script.py /path/to/source.csv /path/to/schema.json /path/to/lookup.csv

## Classes and Methods
Tokenization
A class to handle tokenization and masking of data fields.

__init__(self, source_path: str, schema_path: str, lookup_path: str)
Initializes the Tokenization class with paths for source data, schema, and lookup.

get_spark_session(self) -> SparkSession
Creates and returns a Spark session.

get_schema(self, spark: SparkSession) -> tuple[StructType, list[dict]]
Reads and returns the schema from a JSON file.

_get_spark_type(self, field_type: str) -> DataType
Maps field type from schema to Spark SQL data type.

read_file(self, spark: SparkSession, schema: StructType) -> DataFrame
Reads data from a CSV file into a DataFrame using the provided schema.

read_lookup(self, spark: SparkSession) -> DataFrame
Reads lookup data from a CSV file into a DataFrame.

get_pii_tokenize_fields(self, fields: list[dict]) -> tuple[list[str], list[str]]
Determines which fields are PII and which should be tokenized.

mask_pii_field(self, data: DataFrame, pii_fields: list[str]) -> DataFrame
Masks PII fields in the DataFrame by setting their values to None.

tokenize_field(self, masked_data: DataFrame, tokenize_fields: list[str], lookup: DataFrame) -> DataFrame
Tokenizes fields in the DataFrame based on the lookup data.

validate_record_counts(self, spark: SparkSession, final_data: DataFrame, schema: StructType) -> None
Validates that no records are dropped during data processing by comparing the record count of raw data with the final processed data.

## Contribution
Feel free to contribute by submitting issues or pull requests. Please ensure that your changes adhere to the existing coding style and include appropriate tests.

This `README.md` should provide a clear and concise overview of the project's purpose, setup, and usage, making it easier for others to understand and use your code.
