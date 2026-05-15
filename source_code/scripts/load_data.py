"""
load_data.py

Loads synthetic finance data from CSV files into Snowflake using the
PySpark Snowflake connector. Run this script first to populate the
ACCOUNTS and TRANSACTIONS source tables before executing the pipeline.

Usage:
    spark-submit \
        --packages net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3,\
                   net.snowflake:snowflake-jdbc:3.13.30 \
        load_data.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Snowflake connection options — update before running
# ---------------------------------------------------------------------------
SF_OPTIONS = {
    "sfURL": "YOUR_ACCOUNT.snowflakecomputing.com",
    "sfUser": "YOUR_USER",
    "sfPassword": "YOUR_PASSWORD",
    "sfDatabase": "DEMO_DB",
    "sfSchema": "FINANCE",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "SYSADMIN",
}

SNOWFLAKE_FORMAT = "net.snowflake.spark.snowflake"

# ---------------------------------------------------------------------------
# Explicit schemas — avoids inference issues on CSV load
# ---------------------------------------------------------------------------
ACCOUNTS_SCHEMA = StructType([
    StructField("account_id",    StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("account_type",  StringType(), True),
    StructField("opened_date",   StringType(), True),
    StructField("status",        StringType(), True),
    StructField("credit_limit",  DoubleType(), True),
])

TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id",    StringType(), False),
    StructField("account_id",        StringType(), True),
    StructField("amount",            DoubleType(), True),
    StructField("transaction_type",  StringType(), True),
    StructField("transaction_date",  StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("description",       StringType(), True),
])


def main():
    spark = SparkSession.builder \
        .appName("FinanceDataLoader") \
        .getOrCreate()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data")

    # ------------------------------------------------------------------
    # Load ACCOUNTS
    # ------------------------------------------------------------------
    accounts_path = os.path.join(data_dir, "accounts.csv")
    accounts_df = spark.read \
        .option("header", "true") \
        .schema(ACCOUNTS_SCHEMA) \
        .csv(accounts_path)

    print(f"Loaded {accounts_df.count()} rows from {accounts_path}")

    accounts_df.write \
        .format(SNOWFLAKE_FORMAT) \
        .options(**SF_OPTIONS) \
        .option("dbtable", "ACCOUNTS") \
        .mode("overwrite") \
        .save()

    print("ACCOUNTS written to Snowflake.")

    # ------------------------------------------------------------------
    # Load TRANSACTIONS
    # ------------------------------------------------------------------
    transactions_path = os.path.join(data_dir, "transactions.csv")
    transactions_df = spark.read \
        .option("header", "true") \
        .schema(TRANSACTIONS_SCHEMA) \
        .csv(transactions_path)

    print(f"Loaded {transactions_df.count()} rows from {transactions_path}")

    transactions_df.write \
        .format(SNOWFLAKE_FORMAT) \
        .options(**SF_OPTIONS) \
        .option("dbtable", "TRANSACTIONS") \
        .mode("overwrite") \
        .save()

    print("TRANSACTIONS written to Snowflake.")

    spark.stop()


if __name__ == "__main__":
    main()
