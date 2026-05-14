"""
Sample Snowpark code with various file path patterns for testing
"""

import pandas as pd
from snowflake.snowpark import Session

# Example 1: S3 paths in read operations
sales_df = session.read.csv("s3://company-data/sales/2024/regional_sales.csv")
customers_df = session.read.parquet("s3://company-data/customers/customer_master.parquet")

# Example 2: HDFS paths
hdfs_df = session.read.parquet("hdfs://prod-cluster/warehouse/fact_transactions.parquet")

# Example 3: Write operations
result_df.write.parquet("s3://output-bucket/results/processed_data.parquet")
summary_df.write.csv("hdfs://cluster/reports/summary.csv")

# Example 4: Path variables
input_path = "s3://raw-data/incoming/daily_feed.csv"
output_location = "hdfs://cluster/processed/output.parquet"
staging_dir = "./staging/temp"

# Example 5: F-strings with dynamic paths
environment = "prod"
date = "2024-01-15"
dynamic_path = f"s3://data-{environment}/daily/{date}/transactions.parquet"

# Example 6: Pandas file operations
config_df = pd.read_csv("/local/config/settings.csv")
lookup_df = pd.read_parquet("../shared/reference_data.parquet")

# Example 7: Already converted stage paths (will update prefix)
existing_stage = session.read.csv("@DATA_STAGE/s3/bucket/file.csv")

# Example 8: Relative paths
local_config = session.read.json("./config/parameters.json")
parent_data = session.read.csv("../../archive/historical.csv")
