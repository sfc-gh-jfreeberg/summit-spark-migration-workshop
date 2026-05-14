"""
Order Processing Pipeline
Reads orders and customer tables, joins them, computes order summaries, and writes results.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when
from pyspark.sql.types import DecimalType

from utils.spark_helpers import get_spark_session
from utils.transformations import add_audit_columns


def process_orders(spark):
    """Join orders with customers and produce order summary."""
    # Read from tables
    orders_df = spark.table("db.schema.orders")
    customers_df = spark.table("db.schema.dim_customers")

    # Join orders with customers
    joined_df = orders_df.join(
        customers_df,
        orders_df.customer_id == customers_df.customer_id,
        "inner"
    ).select(
        orders_df.order_id,
        orders_df.customer_id,
        customers_df.full_name,
        orders_df.order_date,
        orders_df.amount.cast(DecimalType(18, 2)),
        orders_df.status
    )

    # Compute summary per customer
    summary_df = joined_df.groupBy("customer_id", "full_name").agg(
        count("order_id").alias("total_orders"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("avg_order_amount"),
        spark_sum(when(col("status") == "completed", 1).otherwise(0)).alias("completed_orders")
    )

    # Add audit columns
    summary_df = add_audit_columns(summary_df)

    # Write to summary table
    summary_df.write \
        .mode("overwrite") \
        .saveAsTable("db.schema.order_summary")

    print(f"Processed {summary_df.count()} customer order summaries")
    return summary_df.count()


def main():
    """Entry point for order processing."""
    spark = get_spark_session("OrderProcessing")
    try:
        process_orders(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
