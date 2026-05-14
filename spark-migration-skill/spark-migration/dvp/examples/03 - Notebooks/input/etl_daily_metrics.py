# Databricks notebook source
# MAGIC %md 
# MAGIC # ETL Daily Metrics
# MAGIC Reads source data from Snowflake and external databases,
# MAGIC transforms and aggregates daily metrics, then syncs results
# MAGIC to the reporting database and S3.

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %run ./pipeline_config $brand="acme"

# COMMAND ----------

if reprocess_all:
    dbutils.notebook.exit("Skipping — full reprocess handled by batch job")

# COMMAND ----------

# MAGIC %md
# MAGIC # Date Range

# COMMAND ----------

if not reprocess_all:
    update_start_date, update_start_month = get_update_window(end_date, lookback_days)

update_start_day = datetime.strptime(update_start_date, '%Y%m%d').strftime('%Y-%m-%d')
update_end_day = datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')

print("update_start_day:", update_start_day)
print("update_end_day:", update_end_day)

# COMMAND ----------

# MAGIC %md
# MAGIC # Source Data

# COMMAND ----------

from pyspark.storagelevel import StorageLevel

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Orders

# COMMAND ----------

df_daily_orders = spark.read \
    .format("snowflake") \
    .options(**sf_options) \
    .option("sfDatabase", "ANALYTICS_DB") \
    .option("sfSchema", "SALES") \
    .option("sfWarehouse", "COMPUTE_WH") \
    .option("query", f"""
        SELECT *
        FROM DAILY_ORDERS
        WHERE ORDER_DATE BETWEEN '{update_start_day}' AND '{update_end_day}'
    """) \
    .load() \
    .persist(StorageLevel.MEMORY_AND_DISK)

df_daily_orders = df_daily_orders \
    .toDF(*[c.lower() for c in df_daily_orders.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Master

# COMMAND ----------

query_stores = """
(SELECT store_id, store_name, region, district, country_code, open_date, status
 FROM dim_store
 WHERE country_code IN ('US', 'CA')) store_alias
"""
df_stores = spark.read.jdbc(url=db_jdbc_url, table=query_stores, properties=db_connection_props)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate by Store and Day

# COMMAND ----------

df_store_daily = df_daily_orders \
    .join(df_stores, "store_id", "inner") \
    .groupBy("store_id", "store_name", "region", "district", "country_code", "order_date") \
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("order_total").alias("total_revenue"),
        F.avg("order_total").alias("avg_order_value"),
        F.countDistinct("customer_id").alias("unique_customers"),
    ) \
    .withColumn("revenue_per_customer", F.col("total_revenue") / F.col("unique_customers")) \
    .withColumn("etl_updated_dt", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Rollup

# COMMAND ----------

df_store_monthly = df_store_daily \
    .withColumn("year_month", F.date_format("order_date", "yyyy-MM")) \
    .groupBy("store_id", "store_name", "region", "district", "country_code", "year_month") \
    .agg(
        F.sum("order_count").alias("order_count"),
        F.sum("total_revenue").alias("total_revenue"),
        F.avg("avg_order_value").alias("avg_order_value"),
        F.sum("unique_customers").alias("unique_customers"),
    ) \
    .withColumn("etl_updated_dt", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Results

# COMMAND ----------

sync = DataSyncHelper(
    jdbc_url=db_jdbc_url,
    connection_props=db_connection_props,
    s3_prefix=data_path_prefix,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Table

# COMMAND ----------

table_name = "store_daily_metrics"
staging_table = f"temp_staging_{table_name}"

sync.overwrite_table(dataframe=df_store_daily, table_name=staging_table, repartition=20)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO store_daily_metrics
# MAGIC SELECT * FROM temp_staging_store_daily_metrics
# MAGIC ON CONFLICT (store_id, order_date)
# MAGIC DO UPDATE SET
# MAGIC   order_count = EXCLUDED.order_count,
# MAGIC   total_revenue = EXCLUDED.total_revenue,
# MAGIC   avg_order_value = EXCLUDED.avg_order_value,
# MAGIC   unique_customers = EXCLUDED.unique_customers,
# MAGIC   revenue_per_customer = EXCLUDED.revenue_per_customer,
# MAGIC   etl_updated_dt = EXCLUDED.etl_updated_dt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Table

# COMMAND ----------

table_name = "store_monthly_metrics"

sync.overwrite_table(dataframe=df_store_monthly, table_name=table_name, repartition=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist to S3

# COMMAND ----------

sync.save_to_s3(
    table_name="store_daily_metrics",
    dataframe=df_store_daily,
    reprocess=reprocess_all,
)

sync.save_to_s3(
    table_name="store_monthly_metrics",
    dataframe=df_store_monthly,
    reprocess=reprocess_all,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE temp_staging_store_daily_metrics

# COMMAND ----------

print("ETL daily metrics completed successfully.")
