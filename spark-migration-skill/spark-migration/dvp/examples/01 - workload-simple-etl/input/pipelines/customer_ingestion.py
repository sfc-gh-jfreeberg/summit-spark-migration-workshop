"""
Customer Ingestion Pipeline
Reads raw customer data from S3 CSV, applies transformations, and writes to a dimension table.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from utils.spark_helpers import get_spark_session
from utils.transformations import clean_string_columns


def build_customer_schema():
    """Define the expected schema for the customer CSV."""
    return StructType([
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("phone", StringType(), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("zip_code", StringType(), nullable=True),
        StructField("credit_limit", DoubleType(), nullable=True),
    ])


def ingest_customers(spark):
    """Read customer CSV from S3 and write to dimension table."""
    schema = build_customer_schema()

    # Read raw data from S3
    raw_df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv("s3://data-lake/raw/customers.csv")

    # Apply transformations
    cleaned_df = clean_string_columns(raw_df, ["first_name", "last_name", "email"])

    transformed_df = cleaned_df \
        .withColumn("full_name", upper(col("first_name") + " " + col("last_name"))) \
        .withColumn("ingested_at", current_timestamp()) \
        .filter(col("customer_id").isNotNull())

    # Write to dimension table
    transformed_df.write \
        .mode("overwrite") \
        .saveAsTable("db.schema.dim_customers")

    row_count = transformed_df.count()
    print(f"Ingested {row_count} customers to db.schema.dim_customers")
    return row_count


if __name__ == "__main__":
    spark = get_spark_session("CustomerIngestion")
    try:
        ingest_customers(spark)
    finally:
        spark.stop()
