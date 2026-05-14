from snowflake.snowpark.modin import plugin
import os
import shutil
import modin.pandas as pd
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col, sum, count, lit, rank, coalesce, abs, round, when
from snowflake.snowpark.window import Window
import pyspark_wrapper # import patch_spark_session_sql

# --- CONFIGURATION ---
input_path_prefix = "input/"
input_extension = ".csv"
# the name of the input items
INPUT_ITEMS : list[str] = [
    "raw_transactions",
    "customer_master",
    "product_catalog",
    "returns_data",
    "exchange_rates",
]

output_path_prefix = "output/"
output_extension = ".parquet"
# key: is the name of the output item, value is the name of the table in the database
OUTPUT_ITEMS: dict[str, str] = {
    "daily_sales_summary": "DAILY_SALES_SUMMARY",
    "customer_clv": "CUSTOMER_CLV",
    "top_categories": "TOP_CATEGORIES",
    "foreign_transactions": "FOREIGN_TRANSACTIONS",
    "unmatched_transactions": "UNMATCHED_TRANSACTIONS",
}

def create_spark_session() -> Session:
    """Initializes and returns a Spark Session.
    
    """
    # Get the current working directory and set warehouse directory explicitly
    # This prevents URL encoding of paths with spaces
    current_dir = os.getcwd()
    warehouse_dir = os.path.join(current_dir, "spark-warehouse")

    session = Session.builder\
        .app_name("ECommerceDataPipeline", True) \
    .getOrCreate()
    session.update_query_tag({"origin":"sf_sit","name":"sma","version":{"major":8,"minor":1,"patch":26},"attributes":{"language":"Python"}})
    return session

def read_inputs(spark: Session) -> dict[str, DataFrame]:
    """Reads CSV files into Spark DataFrames."""
    data_frames = {}
    # for name in INPUT_ITEMS:
    #     # NOTE: In a real environment, you'd load from an S3/ADLS path or directly from a Hive/Delta table
    #     try:
    #         fileName = f"{input_path_prefix}{name}{input_extension}"
    #         df = spark.read.csv(fileName, header=True, inferSchema=True)
    #         data_frames[name] = df
    #         print(f"Loaded {name}: {df.count()} rows")
    #     except Exception as e:
    #         print(f"Error loading {name} at {fileName}: {e}")
    #         data_frames[name] = spark.createDataFrame(pd.DataFrame()) # Return empty DF on failure
    try:
        reader = spark.read.option("header", True).option("inferSchema", True)
        #EWI: SPRKPY1054 => pyspark.sql.readwriter.DataFrameReader.format with argument value "csv" is not supported.
        #EWI: SPRKPY1082 => The pyspark.sql.readwriter.DataFrameReader.load function is not supported. A workaround is to use Snowpark DataFrameReader format specific method instead (avro csv, json, orc, parquet). The path parameter should be a stage location.
        data_frames["raw_transactions"] =  reader.format("csv").load("input/raw_transactions.csv")
        #EWI: SPRKPY1054 => pyspark.sql.readwriter.DataFrameReader.format with argument value "csv" is not supported.
        #EWI: SPRKPY1082 => The pyspark.sql.readwriter.DataFrameReader.load function is not supported. A workaround is to use Snowpark DataFrameReader format specific method instead (avro csv, json, orc, parquet). The path parameter should be a stage location.
        data_frames["customer_master"] = reader.format("csv").load("input/customer_master.csv")
        #EWI: SPRKPY1054 => pyspark.sql.readwriter.DataFrameReader.format with argument value "csv" is not supported.
        #EWI: SPRKPY1082 => The pyspark.sql.readwriter.DataFrameReader.load function is not supported. A workaround is to use Snowpark DataFrameReader format specific method instead (avro csv, json, orc, parquet). The path parameter should be a stage location.
        data_frames["product_catalog"] = reader.format("csv").load("input/product_catalog.csv")
        #EWI: SPRKPY1054 => pyspark.sql.readwriter.DataFrameReader.format with argument value "csv" is not supported.
        #EWI: SPRKPY1082 => The pyspark.sql.readwriter.DataFrameReader.load function is not supported. A workaround is to use Snowpark DataFrameReader format specific method instead (avro csv, json, orc, parquet). The path parameter should be a stage location.
        data_frames["returns_data"] = reader.format("csv").load("input/returns_data.csv")
        #EWI: SPRKPY1054 => pyspark.sql.readwriter.DataFrameReader.format with argument value "csv" is not supported.
        #EWI: SPRKPY1082 => The pyspark.sql.readwriter.DataFrameReader.load function is not supported. A workaround is to use Snowpark DataFrameReader format specific method instead (avro csv, json, orc, parquet). The path parameter should be a stage location.
        data_frames["exchange_rates"] = reader.format("csv").load("input/exchange_rates.csv")
    except Exception as e:
        print(f"Error loading inputs: {e}")


    return data_frames

def run_pipeline(spark: Session, dfs: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Executes the core transformation logic."""

    # -----------------------------------------------------------------
    # A. Data Cleansing and Preparation
    # -----------------------------------------------------------------

    # Identify transactions missing a customer_id (Data Quality Check)
    unmatched_txns = dfs["raw_transactions"].filter(col("customer_id").isNull()) \
        .select("transaction_id", lit("Missing Customer ID").alias("error_message"))

    # Join Transactions with Returns to calculate net revenue
    txns_returns = dfs["raw_transactions"].join(
        dfs["returns_data"],
        on="transaction_id",
        how="left"
    ).withColumn(
        "net_amount", col("transaction_amount") - coalesce(col("return_amount"), lit(0))
    )

    # -----------------------------------------------------------------
    # B. Output 1: Daily Sales Summary (Aggregation)
    # -----------------------------------------------------------------
    daily_summary = txns_returns.groupBy("transaction_date").agg(sum("transaction_amount").alias("total_revenue"), count("transaction_id").alias("total_transactions"), sum(coalesce(col("return_amount"), lit(0))).alias("total_returned_revenue")
    ).withColumnRenamed("transaction_date", "sale_date")

    # -----------------------------------------------------------------
    # C. Output 2: Customer Lifetime Value (CLV)
    # -----------------------------------------------------------------
    customer_spend = txns_returns.groupBy("customer_id").agg(sum("transaction_amount").alias("total_spend"), sum("net_amount").alias("net_spend")
    )

    customer_clv = dfs["customer_master"].join(
        customer_spend,
        on="customer_id",
        how="left"
    ).select(
        "customer_id",
        "customer_name",
        "country", coalesce(col("total_spend"), lit(0.0)).alias("total_spend"), coalesce(col("net_spend"), lit(0.0)).alias("net_spend")
    )

    # -----------------------------------------------------------------
    # D. Output 3: Top 5 Product Categories (Window Function)
    # -----------------------------------------------------------------
    txns_with_category = dfs["raw_transactions"].join(
        dfs["product_catalog"].select("product_id", "category"),
        on="product_id",
        how="inner" # Drop transactions with unknown products
    )

    category_summary = txns_with_category.groupBy("category").agg(count("transaction_id").alias("total_sales_count")
    )

    # Apply RANK() window function to find the top categories
    window_spec = Window.orderBy(col("total_sales_count").desc())
    top_categories = category_summary.withColumn(
        "category_rank", rank().over(window_spec)
    ).filter(col("category_rank") <= 5)

    # -----------------------------------------------------------------
    # E. Output 4: Foreign Currency Transactions (Enrichment/Conversion)
    # -----------------------------------------------------------------
    # For simulation, assume all transactions are in a foreign currency (EUR) and need conversion
    # Real-world logic would be more complex (lookup currency code per transaction)

    foreign_txns_enriched = dfs["raw_transactions"].join(
        dfs["exchange_rates"].filter(col("currency_code") == "EUR").select(col("rate_date").alias("exchange_date"), "exchange_rate"), col("transaction_date") == col("exchange_date"), # Join on date to get the correct rate
        how="inner"
    ).withColumn(
        "transaction_amount_usd", round(col("transaction_amount") * col("exchange_rate"), 4)
    ).select(
        "transaction_id",
        "transaction_date",
        "transaction_amount_usd"
    )

    # -----------------------------------------------------------------
    # F. Output 5: Unmatched Transactions (Error Log)
    # -----------------------------------------------------------------
    # Use the unmatched_txns from the cleansing step (A)
    # Filter: Transactions with amounts > 500 AND those missing a customer ID
    big_unmatched_txns = dfs["raw_transactions"].filter(( col("transaction_amount") > 500) | ( col("customer_id").isNull())) \
        .withColumn(
            "error_message",
            ( when(col("transaction_amount") > 500, "High Value Transaction (>500)").otherwise(lit("Missing Customer ID"))
            )
        ).select("transaction_id", "error_message").distinct()

    return {
        OUTPUT_ITEMS["daily_sales_summary"]: daily_summary,
        OUTPUT_ITEMS["customer_clv"]: customer_clv,
        OUTPUT_ITEMS["top_categories"]: top_categories,
        OUTPUT_ITEMS["foreign_transactions"]: foreign_txns_enriched,
        OUTPUT_ITEMS["unmatched_transactions"]: big_unmatched_txns,
    }

def cleanup_existing_table_directory(spark: Session, table_name: str) -> None:
    """Removes an existing table physical directory to avoid LOCATION_ALREADY_EXISTS error."""

    spark.sql(f"""DROP TABLE IF EXISTS {table_name};""")
    # Also remove the physical directory if it exists (managed tables store data in directories)
    warehouse_dir = os.path.join(os.getcwd(), "spark-warehouse", table_name.lower())

    if os.path.exists(warehouse_dir):
        try:
            shutil.rmtree(warehouse_dir)
            print(f"Removed existing directory: {warehouse_dir}")
        except Exception as e:
            print(f"Warning: Could not remove directory {warehouse_dir}: {e}")

def write_outputs(spark: Session, output_dfs: dict[str, DataFrame]) -> None:
    """Writes the output DataFrames to the target tables.
    
    Args:
        spark: SparkSession
        output_dfs: Dictionary of table names to DataFrames
    """
    for table_name, df in output_dfs.items():
        print(f"\n--- Writing to Output Table: {table_name} ---")
        df.show(5)

        # Write to Parquet files (default Spark behavior)
        cleanup_existing_table_directory(spark, table_name)
        df.write.mode("overwrite").saveAsTable(table_name)
        # df.write.mode("overwrite").csv(table_name)
        print(f"✓ Saved {table_name} as Parquet table")

def show_outputs(spark: Session, output_dfs: dict[str, DataFrame]) -> None:
    """Shows the contents of the output tables."""

    for table_name in output_dfs.keys():
        show_parquet_file(spark, table_name)

def show_parquet_file(spark: Session, table_name):
    """Shows the contents of a Parquet file."""
    #EWI: SPRKPY1029 => This issue appears when the tool detects the usage of pyspark.sql.readwriter.DataFrameReader.parquet. This function is supported, but some of the differences between Snowpark and the Spark API might require making some manual changes.
    df = spark.read.parquet(f'spark-warehouse/{table_name}')
    print(f'\n--- Table: {table_name} (Parquet) ---')
    print('Schema:')
    df.printSchema()
    print('\nData:')
    df.show(truncate=False)

def show_outputs_using_sql(spark: Session, output_dfs: list[str]) -> None:
    """Shows the contents of the output tables by executing SQL queries."""

    for table_name in output_dfs:
        spark.sql(f"""SELECT
  *
FROM
  {table_name};""").show()

    spark.sql("""--** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "foreign_transactions" **
SELECT
  date_format(transaction_date, 'yyyy-MM-dd') !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'date_format' NODE ***/!!! as transaction_date,
  format_number(transaction_amount_usd, 4) !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'format_number' NODE ***/!!! as transaction_amount_usd
FROM
  foreign_transactions;"""
            ).show()

def read_existing_tables(spark: Session) -> dict[str, DataFrame]:
    """Reads existing Parquet tables from spark-warehouse and registers them as Spark tables.
    
    Returns:
        Dictionary of table names to DataFrames
    """
    warehouse_dir = os.path.join(os.getcwd(), "spark-warehouse")
    existing_tables = {}

    # Get list of output table names (values from OUTPUT_ITEMS)
    table_names = list(OUTPUT_ITEMS.values())

    for table_name in table_names:
        # Check both lowercase and original case
        table_dir_lower = os.path.join(warehouse_dir, table_name.lower())
        table_dir_original = os.path.join(warehouse_dir, table_name)

        # Try to find the table directory
        table_dir = None
        if os.path.exists(table_dir_lower):
            table_dir = table_dir_lower
        elif os.path.exists(table_dir_original):
            table_dir = table_dir_original

        if table_dir and os.path.exists(os.path.join(table_dir, "_SUCCESS")):
            try:
                #EWI: SPRKPY1029 => This issue appears when the tool detects the usage of pyspark.sql.readwriter.DataFrameReader.parquet. This function is supported, but some of the differences between Snowpark and the Spark API might require making some manual changes.
                df = spark.read.parquet(table_dir)

                # Register as a temporary view/table so it can be queried with SQL
                # df.createOrReplaceTempView(table_name.lower() + '_temp')
                # spark.sql(f"SELECT * FROM {table_name.lower()}_temp").show()

                # Also try to register with original case
                try:
                    spark.sql(f"""--** SSC-FDM-0019 - SEMANTIC INFORMATION COULD NOT BE LOADED FOR {table_name}. CHECK IF THE NAME IS INVALID OR DUPLICATED. **
CREATE TABLE IF NOT EXISTS {table_name}
USING PARQUET !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'Using' NODE ***/!!!
LOCATION '{table_dir}' !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'Location' NODE ***/!!!
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 8,  "minor": 1,  "patch": "26.0" }, "attributes": {  "component": "spark",  "convertedOn": "02/12/2026",  "domain": "snowflake",  "migrationid": "GFOcAel2cnOzaYfFekw/MA==" }}'
;""")
                    spark.sql(f"""SELECT
  *
FROM
  {table_name};""").show()
                except Exception:
                    pass

                existing_tables[table_name] = df
                print(f"✓ Loaded existing table: {table_name} ({df.count()} rows)")
            except Exception as e:
                print(f"Warning: Could not load table {table_name} from {table_dir}: {e}")
        else:
            print(f"Table {table_name} not found in spark-warehouse")

    return existing_tables

if __name__ == "__main__":
    spark = create_spark_session()

    # For actual execution, ensure the CSV files are in the input directory.
    # The read_inputs function will attempt to load them.

    # Create the dummy files (for successful run in a local environment)
    # (The creation of the dummy files is detailed in the next section)

    print("\n=== Load csv files into memory ===")
    input_dfs = read_inputs(spark)

    print("\n=== Checking for existing tables in spark-warehouse ===")
    # existing_output_tables = read_existing_tables(spark)

    if all(df is not None for df in input_dfs.values()):
        output_dfs = run_pipeline(spark, input_dfs)
        write_outputs(spark, output_dfs)
        # show_outputs(spark, output_dfs)
        show_outputs_using_sql(spark, output_dfs.keys())
    else:
        print("\n*** ERROR: Could not load all input tables. Pipeline aborted. ***")

    spark.close()

    # To run this script locally with Spark installed, save it as data_pipeline.py and run:
    # spark-submit data_pipeline.py