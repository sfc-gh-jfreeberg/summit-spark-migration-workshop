"""
transaction_pipeline.py

PySpark financial transaction analysis pipeline.

Reads ACCOUNTS and TRANSACTIONS from Snowflake, applies a series of
transformations, and writes MONTHLY_SPEND_SUMMARY back to Snowflake.

Transformations demonstrated:
  - Filter & Select  : keep only ACTIVE accounts and DEBIT/TRANSFER transactions
  - UDFs             : amount size categorisation, fraud-flag heuristic
  - Aggregation      : monthly spend grouped by account and merchant category
  - Window functions : cumulative spend, spend rank, month-over-month lag

Usage:
    spark-submit \
        --packages net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3,\
                   net.snowflake:snowflake-jdbc:3.13.30 \
        transaction_pipeline.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

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
# UDF: categorise a transaction by its amount
# ---------------------------------------------------------------------------
def categorize_amount(amount):
    """Return a size-band label for a transaction amount."""
    if amount is None:
        return "unknown"
    if amount < 50.0:
        return "small"
    elif amount < 500.0:
        return "medium"
    else:
        return "large"


categorize_amount_udf = F.udf(categorize_amount, StringType())


# ---------------------------------------------------------------------------
# UDF: simple fraud-flag heuristic
# ---------------------------------------------------------------------------
def fraud_flag(account_type, amount):
    """Flag CREDIT account transactions above $3,000 for review."""
    if account_type == "CREDIT" and amount is not None and amount > 3000.0:
        return "REVIEW"
    return "OK"


fraud_flag_udf = F.udf(fraud_flag, StringType())


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def main():
    spark = SparkSession.builder \
        .appName("FinancialTransactionPipeline") \
        .getOrCreate()

    # ------------------------------------------------------------------
    # 1. Read source tables from Snowflake
    # ------------------------------------------------------------------
    accounts_df = spark.read \
        .format(SNOWFLAKE_FORMAT) \
        .options(**SF_OPTIONS) \
        .option("dbtable", "ACCOUNTS") \
        .load()

    transactions_df = spark.read \
        .format(SNOWFLAKE_FORMAT) \
        .options(**SF_OPTIONS) \
        .option("dbtable", "TRANSACTIONS") \
        .load()

    # ------------------------------------------------------------------
    # 2. Filter & Select
    # ------------------------------------------------------------------
    # Keep only ACTIVE accounts with columns needed downstream
    active_accounts = accounts_df \
        .filter(F.col("status") == "ACTIVE") \
        .select(
            "account_id",
            "customer_name",
            "account_type",
            F.col("credit_limit").cast("double").alias("credit_limit"),
        )

    # Keep DEBIT and TRANSFER transactions with a valid amount
    filtered_txns = transactions_df \
        .filter(F.col("transaction_type").isin("DEBIT", "TRANSFER")) \
        .filter(F.col("amount").isNotNull()) \
        .select(
            "transaction_id",
            "account_id",
            F.col("amount").cast("double").alias("amount"),
            "transaction_type",
            F.to_date("transaction_date", "yyyy-MM-dd").alias("transaction_date"),
            "merchant_category",
            "description",
        )

    # ------------------------------------------------------------------
    # 3. Apply UDFs
    # ------------------------------------------------------------------
    enriched_txns = filtered_txns \
        .withColumn("amount_category", categorize_amount_udf(F.col("amount")))

    # ------------------------------------------------------------------
    # 4. Join transactions onto active accounts
    # ------------------------------------------------------------------
    joined_df = enriched_txns.join(active_accounts, on="account_id", how="inner")

    # Fraud flag requires account_type from the joined accounts side
    joined_df = joined_df.withColumn(
        "fraud_flag",
        fraud_flag_udf(F.col("account_type"), F.col("amount")),
    )

    # ------------------------------------------------------------------
    # 5. Aggregation — monthly spend by account and merchant category
    # ------------------------------------------------------------------
    monthly_spend = joined_df \
        .withColumn("year_month", F.date_format(F.col("transaction_date"), "yyyy-MM")) \
        .groupBy(
            "account_id",
            "customer_name",
            "account_type",
            "credit_limit",
            "year_month",
            "merchant_category",
        ) \
        .agg(
            F.sum("amount").alias("total_spend"),
            F.count("transaction_id").alias("transaction_count"),
            F.avg("amount").alias("avg_transaction_amount"),
            F.max("amount").alias("max_transaction_amount"),
            F.sum(
                F.when(F.col("fraud_flag") == "REVIEW", 1).otherwise(0)
            ).alias("flagged_transactions"),
        )

    # ------------------------------------------------------------------
    # 6. Window functions
    # ------------------------------------------------------------------
    # Running cumulative spend per account, ordered chronologically
    account_month_window = Window \
        .partitionBy("account_id") \
        .orderBy("year_month") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Global rank of every (account, month, category) row by spend
    global_rank_window = Window.orderBy(F.col("total_spend").desc())

    # Month-over-month comparison within each account + category slice
    lag_window = Window \
        .partitionBy("account_id", "merchant_category") \
        .orderBy("year_month")

    result_df = monthly_spend \
        .withColumn(
            "cumulative_spend",
            F.sum("total_spend").over(account_month_window),
        ) \
        .withColumn(
            "spend_rank",
            F.rank().over(global_rank_window),
        ) \
        .withColumn(
            "prev_month_spend",
            F.lag("total_spend", 1).over(lag_window),
        ) \
        .withColumn(
            "month_over_month_change",
            F.col("total_spend") - F.col("prev_month_spend"),
        ) \
        .withColumn(
            "credit_utilisation_pct",
            F.round(
                (F.col("cumulative_spend") / F.col("credit_limit")) * 100, 2
            ),
        )

    # ------------------------------------------------------------------
    # 7. Write results back to Snowflake
    # ------------------------------------------------------------------
    result_df.write \
        .format(SNOWFLAKE_FORMAT) \
        .options(**SF_OPTIONS) \
        .option("dbtable", "MONTHLY_SPEND_SUMMARY") \
        .mode("overwrite") \
        .save()

    print(f"Pipeline complete. Wrote {result_df.count()} rows to MONTHLY_SPEND_SUMMARY.")

    spark.stop()


if __name__ == "__main__":
    main()
