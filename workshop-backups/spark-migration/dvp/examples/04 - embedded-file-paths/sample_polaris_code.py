#!/usr/bin/env python3
"""
Test file demonstrating Polaris catalog path detection
Polaris is Snowflake's open-source Apache Iceberg catalog
"""

from snowflake.snowpark import Session
import polars as pl
from polaris.catalog import PolarisClient

# Initialize Polaris catalog client
polaris_client = PolarisClient(
    host="https://polaris.example.com",
    credentials="s3://polaris-config/credentials.json"
)

# Example 1: Polaris table with S3 backend
polaris_table = polaris_client.load_table("catalog.schema.table")
table_location = "s3://polaris-warehouse/iceberg/catalog/schema/table"

# Example 2: Reading from Polaris-managed Iceberg tables
df_iceberg = spark.read \
    .format("iceberg") \
    .load("s3://polaris-data/iceberg/sales_table")

# Example 3: Polaris external volume paths
external_volume = session.sql("""
    CREATE EXTERNAL VOLUME polaris_volume
    STORAGE_LOCATIONS = (
        (
            NAME = 's3_location'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://polaris-external/volumes/'
        )
    )
""")

# Example 4: Polaris catalog metadata paths
catalog_metadata = polaris_client.get_catalog("main")
metadata_path = "s3://polaris-catalog/metadata/catalog.json"

# Example 5: Iceberg table metadata
iceberg_metadata_location = "s3://polaris-warehouse/iceberg/metadata/v1.metadata.json"

# Example 6: Polaris with HDFS backend
polaris_hdfs_table = spark.read \
    .format("iceberg") \
    .option("catalog", "polaris") \
    .load("hdfs://polaris-cluster/warehouse/iceberg/fact_sales")

# Example 7: Polaris snapshot paths
snapshot_path = "s3://polaris-warehouse/iceberg/snapshots/snap-12345.avro"

# Example 8: Reading Polaris catalog config
config_file = pl.read_json("./polaris-config/catalog.json")

# Example 9: Polaris manifest files
manifest_location = "s3://polaris-data/iceberg/manifests/manifest-12345.avro"

# Example 10: Polaris with Azure storage
polaris_azure = spark.read \
    .format("iceberg") \
    .load("abfss://polaris-container@account.dfs.core.windows.net/iceberg/tables/sales")

# Example 11: Polaris with GCS
polaris_gcs = spark.read \
    .format("iceberg") \
    .option("catalog", "polaris") \
    .load("gs://polaris-bucket/iceberg/warehouse/customers")

# Example 12: Dynamic Polaris paths with variables
environment = "production"
catalog_name = "analytics"
polaris_dynamic = f"s3://polaris-{environment}/{catalog_name}/tables/data.parquet"

# Example 13: Polaris REST catalog endpoint config
rest_catalog_config = {
    "uri": "https://polaris.example.com/api/catalog",
    "warehouse": "s3://polaris-warehouse/default",
    "credential": "./polaris-config/token.json"
}

# Example 14: Write to Polaris-managed location
result_df.write \
    .format("iceberg") \
    .mode("append") \
    .save("s3://polaris-output/iceberg/processed/results")

# Example 15: Polaris table with multiple data file paths
data_files = [
    "s3://polaris-data/iceberg/data/file-001.parquet",
    "s3://polaris-data/iceberg/data/file-002.parquet",
    "s3://polaris-data/iceberg/data/file-003.parquet"
]

# Example 16: Polaris vacuum/cleanup paths
vacuum_location = "s3://polaris-warehouse/iceberg/.trash/deleted-files"

# Example 17: Reading Polaris table properties
table_props = polaris_client.load_table(
    "s3://polaris-warehouse/iceberg/metadata/v2.metadata.json"
)

# Example 18: Polaris with file:// protocol (local testing)
local_polaris = spark.read \
    .format("iceberg") \
    .load("file:///tmp/polaris-local/warehouse/test_table")

# Example 19: Relative paths for Polaris config
polaris_local_config = "./config/polaris-catalog.yaml"
polaris_credentials = "../secrets/polaris-token.json"

# Example 20: Polaris materialized view paths
mv_location = "s3://polaris-warehouse/iceberg/views/sales_summary_mv"
