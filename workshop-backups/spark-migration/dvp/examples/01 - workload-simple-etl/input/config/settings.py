"""
Configuration settings for the ETL workload.
"""

# Database settings
DATABASE = "db"
SCHEMA = "schema"

# S3 paths
S3_RAW_BUCKET = "s3://data-lake/raw"
S3_WAREHOUSE = "s3://data-lake/warehouse"

# Processing settings
BATCH_SIZE = 10000
MAX_RETRIES = 3
SHUFFLE_PARTITIONS = 200
