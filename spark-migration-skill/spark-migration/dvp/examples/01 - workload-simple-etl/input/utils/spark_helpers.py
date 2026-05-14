"""
Shared Spark helper functions.
"""
from pyspark.sql import SparkSession


def get_spark_session(app_name):
    """Create or get a Spark session with standard configuration."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.warehouse.dir", "s3://data-lake/warehouse") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    return spark


def log_dataframe_info(df, label="DataFrame"):
    """Log basic info about a DataFrame."""
    print(f"[{label}] Schema:")
    df.printSchema()
    print(f"[{label}] Row count: {df.count()}")
    print(f"[{label}] Partitions: {df.rdd.getNumPartitions()}")
