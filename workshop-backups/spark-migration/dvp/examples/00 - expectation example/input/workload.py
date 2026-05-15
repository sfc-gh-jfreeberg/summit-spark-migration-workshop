import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, lit, rank, coalesce, round, when
from pyspark.sql.window import Window

INPUT_PATH = os.getenv("INPUT_DATA_PATH", "input")

def create_spark_session() -> SparkSession:
    app_name = os.getenv("SPARK_APP_NAME") or "ECommerceDataPipeline"
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()
        print(f"Created Spark session: {app_name}")
        return spark
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        return None

def run_pipeline(spark: SparkSession) -> list[str]:
    """Run the transformation pipeline and write outputs as they're ready.
    
    Returns list of output table names that were written.
    """
    reader = spark.read.option("header", True).option("inferSchema", True)

    # Read raw transactions - used for multiple transformations
    raw_transactions = reader.csv(f"{INPUT_PATH}/raw_transactions.csv")

    # Read returns data for transaction adjustments
    returns_data = reader.csv(f"{INPUT_PATH}/returns_data.csv")

    # Join transactions with returns to calculate net amounts
    txns_returns = raw_transactions.join(
        returns_data, 
        on="transaction_id", 
        how="left"
    ).withColumn(
        "net_amount", 
        col("transaction_amount") - coalesce(col("return_amount"), lit(0))
    )
    
    # Calculate daily sales summary and write immediately
    daily_summary = txns_returns.groupBy("transaction_date").agg(
        sum("transaction_amount").alias("total_revenue"),
        count("transaction_id").alias("total_transactions"),
        sum(coalesce(col("return_amount"), lit(0))).alias("total_returned_revenue")
    ).withColumnRenamed("transaction_date", "sale_date")
    daily_summary.write.mode("overwrite").csv("output/DAILY_SALES_SUMMARY", header=True)
    print("Written: DAILY_SALES_SUMMARY")
    
    # Calculate customer spending for CLV
    customer_spend = txns_returns.groupBy("customer_id").agg(
        sum("transaction_amount").alias("total_spend"),
        sum("net_amount").alias("net_spend")
    )
    
    # Read customer master from table
    customer_master = spark.table("CUSTOMER_MASTER")
    
    customer_clv = customer_master.join(
        customer_spend, 
        on="customer_id", 
        how="left"
    ).select(
        "customer_id",
        "customer_name",
        "country",
        coalesce(col("total_spend"), lit(0.0)).alias("total_spend"),
        coalesce(col("net_spend"), lit(0.0)).alias("net_spend")
    )
    customer_clv.write.mode("overwrite").csv("output/CUSTOMER_CLV", header=True)
    print("Written: CUSTOMER_CLV")

    # Read product catalog from table
    product_catalog = spark.table("PRODUCT_CATALOG")
    
    txns_with_category = raw_transactions.join(
        product_catalog.select("product_id", "category"),
        on="product_id", 
        how="inner"
    )
    
    category_summary = txns_with_category.groupBy("category").agg(
        count("transaction_id").alias("total_sales_count")
    )
    
    window_spec = Window.orderBy(col("total_sales_count").desc())
    top_categories = category_summary.withColumn(
        "category_rank",
        rank().over(window_spec)
    ).filter(col("category_rank") <= 5)
    top_categories.write.mode("overwrite").saveAsTable("TOP_CATEGORIES")
    print("Written: TOP_CATEGORIES")
    
    # Read exchange rates for foreign transaction conversion
    exchange_rates = reader.csv(f"{INPUT_PATH}/exchange_rates.csv")
    
    foreign_txns_enriched = raw_transactions.join(
        exchange_rates.filter(col("currency_code") == "EUR").select(col("rate_date").alias("exchange_date"), "exchange_rate"),
        col("transaction_date") == col("exchange_date"),
        how="inner"
    ).withColumn(
        "transaction_amount_usd",
        round(col("transaction_amount") * col("exchange_rate"), 4)
    ).select(
        "transaction_id",
        "transaction_date",
        "transaction_amount_usd"
    )
    foreign_txns_enriched.write.mode("overwrite").csv("output/FOREIGN_TRANSACTIONS", header=True)
    print("Written: FOREIGN_TRANSACTIONS")
    
    # Identify unmatched/problematic transactions
    big_unmatched_txns = raw_transactions.filter((col("transaction_amount") > 500) | (col("customer_id").isNull())) \
        .withColumn(
            "error_message",
            when(col("transaction_amount") > 500, "High Value Transaction (>500)")
            .otherwise(lit("Missing Customer ID"))
        ).select("transaction_id", "error_message").distinct()
    big_unmatched_txns.write.mode("overwrite").saveAsTable("UNMATCHED_TRANSACTIONS")
    print("Written: UNMATCHED_TRANSACTIONS")

    return [
        "DAILY_SALES_SUMMARY",
        "CUSTOMER_CLV",
        "TOP_CATEGORIES",
        "FOREIGN_TRANSACTIONS",
        "UNMATCHED_TRANSACTIONS",
    ]

OUTPUT_FILES = ["DAILY_SALES_SUMMARY", "CUSTOMER_CLV", "FOREIGN_TRANSACTIONS"]
OUTPUT_TABLES = ["TOP_CATEGORIES", "UNMATCHED_TRANSACTIONS"]

def verify_outputs(spark: SparkSession, output_names: list[str]) -> None:
    print("\n=== Verifying Outputs ===")
    
    for name in output_names:
        try:
            if name in OUTPUT_TABLES:
                df = spark.table(name)
            else:
                df = spark.read.option("header", True).csv(f"output/{name}")
            row_count = df.count()
            short_name = name.split(".")[-1] if "." in name else name
            print(f"{short_name}: {row_count} rows")
        except Exception as e:
            print(f"{name}: Not found or error - {e}")

def show_outputs(spark: SparkSession, output_names: list[str]) -> None:
    for name in output_names:
        try:
            short_name = name.split(".")[-1] if "." in name else name
            print(f'\n--- Output: {short_name} ---')
            if name in OUTPUT_TABLES:
                df = spark.table(name)
            else:
                df = spark.read.option("header", True).csv(f"output/{name}")
            df.show()
        except Exception as e:
            print(f"Error reading output {name}: {e}")

def main():
    should_stop = False
    
    try:
        print("\n" + "="*70)
        print("E-Commerce Data Pipeline - PySpark Edition")
        print("="*70)
        
        spark = create_spark_session()
        should_stop = True
        if spark is None:
            print("ERROR: Failed to create Spark session")
            return 1
        
        print("\n=== Step 1: Run Transformation Pipeline ===")
        output_names = run_pipeline(spark)
        print(f"Written {len(output_names)} outputs")
        
        print("\n=== Step 2: Verify Outputs ===")
        verify_outputs(spark, output_names)
        
        print("\n=== Step 3: Display Results ===")
        show_outputs(spark, output_names)
        
        print("\n" + "="*70)
        print("Pipeline completed successfully!")
        print("="*70)
        return 0
        
    except Exception as e:
        print("\n" + "="*70)
        print("Pipeline failed with error:")
        print("="*70)
        print(f"{type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        if should_stop and spark is not None:
            try:
                print("\nStopping Spark session...")
                spark.stop()
                print("Spark session stopped")
            except Exception as e:
                print(f"Warning: Error stopping Spark session: {e}")


if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)
