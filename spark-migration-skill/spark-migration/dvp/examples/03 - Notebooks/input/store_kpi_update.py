# Databricks notebook source
# MAGIC %md
# MAGIC # Store KPI Update
# MAGIC Loads performance data, aggregates store-level KPIs (customer satisfaction,
# MAGIC speed of service, order accuracy), and syncs results to Snowflake.
# MAGIC
# MAGIC Depends on `store_config` for environment, credentials, and utility functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ./store_config $region="NA"

# COMMAND ----------

if reprocess_all:
    dbutils.notebook.exit("Skipping — full reprocess handled by batch job")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Range

# COMMAND ----------

if not reprocess_all:
    update_start_date, update_start_month = get_update_window(end_date, lookback_days)

update_start_quarter = year_month_to_quarter(update_start_month)

print("update_start_date:", update_start_date)
print("update_start_month:", update_start_month)
print("update_start_quarter:", update_start_quarter)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Data

# COMMAND ----------

store_info = spark\
    .read\
    .option("multiLine", "True")\
    .option("header", "True")\
    .csv(f"{path_prefix}/store_info")\
    .filter(F.col("quarter") >= update_start_quarter)

temp_closed = store_info\
    .filter(F.col("temp_closure") == "Y")\
    .select("store_id", "year_month")\
    .distinct()

print("Store info loaded:", store_info.count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Satisfaction (CSAT)

# COMMAND ----------

csat_raw = spark\
    .read\
    .option("multiLine", "True")\
    .option("header", "True")\
    .csv(f"{path_prefix}/csat_surveys")\
    .filter(F.col("year_month") >= update_start_month)\
    .drop("etl_updated_dt")\
    .join(temp_closed, ["store_id", "year_month"], "leftanti")

csat_threshold = 30

w_quarter = Window.partitionBy("store_id", "quarter")

csat_monthly = csat_raw\
    .groupBy("store_id", "quarter", "year_month")\
    .agg(
        F.sum(F.col("total_responses")).alias("total_responses"),
        F.sum(F.col("positive_responses")).alias("positive_responses"),
        F.sum(F.col("total_tickets")).alias("total_tickets"),
    )\
    .withColumn(
        "responses_quarter",
        F.sum(F.col("total_responses")).over(w_quarter)
    )\
    .withColumn(
        "csat_score",
        F.when(F.col("total_tickets").isNull(), F.lit(None))
         .when(F.col("total_tickets") == 0, F.lit(0))
         .otherwise(F.round(F.col("positive_responses") / F.col("total_tickets") * 100, 4))
    )\
    .withColumn(
        "csat_penalty",
        F.when(
            (F.col("responses_quarter") / 4 < csat_threshold),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )

csat_quarterly = csat_monthly\
    .groupBy("store_id", "quarter", "csat_penalty")\
    .agg(
        F.sum("total_responses").alias("total_responses"),
        F.sum("positive_responses").alias("positive_responses"),
        F.sum("total_tickets").alias("total_tickets"),
    )\
    .withColumn(
        "csat_score",
        F.when(F.col("total_tickets") == 0, F.lit(0))
         .otherwise(F.round(F.col("positive_responses") / F.col("total_tickets") * 100, 4))
    )

print("CSAT aggregated:", csat_quarterly.count(), "quarterly rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Speed of Service (SOS)

# COMMAND ----------

sos_raw = spark\
    .read\
    .option("multiLine", "True")\
    .option("header", "True")\
    .csv(f"{path_prefix}/speed_of_service")\
    .filter(F.col("year_month") >= update_start_month)\
    .drop("etl_updated_dt")\
    .join(temp_closed, ["store_id", "year_month"], "leftanti")

sos_monthly = sos_raw\
    .groupBy("store_id", "quarter", "year_month")\
    .agg(
        F.sum(F.col("total_wait_seconds")).alias("total_wait_seconds"),
        F.sum(F.col("order_count")).alias("order_count"),
    )\
    .withColumn(
        "avg_wait_seconds",
        F.round(F.col("total_wait_seconds") / F.col("order_count"))
    )

sos_quarterly = sos_raw\
    .groupBy("store_id", "quarter")\
    .agg(
        F.sum("total_wait_seconds").alias("total_wait_seconds"),
        F.sum("order_count").alias("order_count"),
    )\
    .withColumn(
        "avg_wait_seconds",
        F.round(F.col("total_wait_seconds") / F.col("order_count"))
    )

print("SOS aggregated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Accuracy

# COMMAND ----------

accuracy_raw = spark\
    .read\
    .option("multiLine", "True")\
    .option("header", "True")\
    .csv(f"{path_prefix}/order_accuracy")\
    .filter(F.col("year_month") >= update_start_month)\
    .drop("etl_updated_dt")\
    .join(temp_closed, ["store_id", "year_month"], "leftanti")

accuracy_monthly = accuracy_raw\
    .groupBy("store_id", "quarter", "year_month")\
    .agg(
        F.sum("inaccurate_orders").alias("inaccurate_orders"),
        F.sum("total_orders").alias("total_orders"),
    )\
    .withColumn(
        "inaccuracy_rate",
        F.when(F.col("total_orders") == 0, F.lit(0))
         .otherwise(F.round(F.col("inaccurate_orders") / F.col("total_orders") * 100, 4))
    )

accuracy_quarterly = accuracy_raw\
    .groupBy("store_id", "quarter")\
    .agg(
        F.sum("inaccurate_orders").alias("inaccurate_orders"),
        F.sum("total_orders").alias("total_orders"),
    )\
    .withColumn(
        "inaccuracy_rate",
        F.when(F.col("total_orders") == 0, F.lit(0))
         .otherwise(F.round(F.col("inaccurate_orders") / F.col("total_orders") * 100, 4))
    )

print("Order accuracy aggregated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Details (External DB)

# COMMAND ----------

query = """
(SELECT store_id, store_name, region, district, city, state,
        open_date, has_drive_thru, ownership_type
 FROM ops_schema.dim_store
 WHERE region = 'NA' AND status = 'ACTIVE') store_alias
"""

dim_store = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=connection_properties
)

query_district = """
(SELECT district_id, district_name, area_manager
 FROM ops_schema.dim_district
 WHERE region = 'NA') district_alias
"""

dim_district = spark.read.jdbc(
    url=jdbc_url,
    table=query_district,
    properties=connection_properties
)

print("Store dimension loaded:", dim_store.count(), "stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine KPIs & Compute Star Ratings

# COMMAND ----------

star_thresholds = spark\
    .read\
    .option("multiLine", "True")\
    .option("header", "True")\
    .csv(f"{path_prefix}/star_thresholds")\
    .drop("created_at")

kpi_summary = store_info\
    .join(csat_quarterly, ["store_id", "quarter"], "left")\
    .join(sos_quarterly, ["store_id", "quarter"], "left")\
    .join(accuracy_quarterly, ["store_id", "quarter"], "left")\
    .join(star_thresholds, ["region", "quarter"], "left")\
    .withColumn(
        "csat_star",
        F.when(F.col("csat_score").isNull(), F.lit(None))
         .when(F.col("csat_score") >= F.col("csat_5star"), F.lit(5))
         .when(F.col("csat_score") >= F.col("csat_4star"), F.lit(4))
         .when(F.col("csat_score") >= F.col("csat_3star"), F.lit(3))
         .when(F.col("csat_score") >= F.col("csat_2star"), F.lit(2))
         .otherwise(F.lit(1))
    )\
    .withColumn(
        "sos_star",
        F.when(F.col("avg_wait_seconds").isNull(), F.lit(None))
         .when(F.col("avg_wait_seconds") <= F.col("sos_5star"), F.lit(5))
         .when(F.col("avg_wait_seconds") <= F.col("sos_4star"), F.lit(4))
         .when(F.col("avg_wait_seconds") <= F.col("sos_3star"), F.lit(3))
         .when(F.col("avg_wait_seconds") <= F.col("sos_2star"), F.lit(2))
         .otherwise(F.lit(1))
    )\
    .withColumn(
        "accuracy_star",
        F.when(F.col("inaccuracy_rate").isNull(), F.lit(None))
         .when(F.col("inaccuracy_rate") <= F.col("accuracy_5star"), F.lit(5))
         .when(F.col("inaccuracy_rate") <= F.col("accuracy_4star"), F.lit(4))
         .when(F.col("inaccuracy_rate") <= F.col("accuracy_3star"), F.lit(3))
         .when(F.col("inaccuracy_rate") <= F.col("accuracy_2star"), F.lit(2))
         .otherwise(F.lit(1))
    )\
    .withColumn(
        "overall_star",
        F.round(
            (F.coalesce(F.col("csat_star"), F.lit(0)) * 0.4
             + F.coalesce(F.col("sos_star"), F.lit(0)) * 0.35
             + F.coalesce(F.col("accuracy_star"), F.lit(0)) * 0.25),
            2
        )
    )\
    .withColumn("etl_updated_dt", F.current_timestamp())

print("KPI summary computed:", kpi_summary.count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Snowflake

# COMMAND ----------

table_name = "store_kpi_quarterly"
snowflake_update(
    table_name=table_name,
    time_index="quarter",
    update_start_time=update_start_quarter,
)

# COMMAND ----------

table_name = "store_kpi_monthly"
snowflake_update(
    table_name=table_name,
    time_index="year_month",
    update_start_time=update_start_month,
)

# COMMAND ----------

table_name = "dim_store_details"
snowflake_update(
    table_name=table_name,
    time_index=None,
    update_start_time=None,
)

# COMMAND ----------

dbutils.notebook.exit("Store KPI update completed successfully")
