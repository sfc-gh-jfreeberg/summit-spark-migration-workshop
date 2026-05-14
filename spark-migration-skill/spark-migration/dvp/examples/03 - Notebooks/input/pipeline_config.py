# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Configuration
# MAGIC Shared configuration loaded by all pipeline notebooks via `%run`.
# MAGIC Sets up credentials, date ranges, paths, and reusable utilities.

# COMMAND ----------

import json
import os
from datetime import datetime, timedelta, date
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

print("Packages imported")

spark.conf.set("spark.sql.caseSensitive", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment & Credentials

# COMMAND ----------

TASK_KEY = "init_config"
PROJECT_NAME = "analytics"
PRODUCT_NAME = "metrics"

ENV = dbutils.jobs.taskValues.get(
    taskKey=TASK_KEY, key="ENV", default="dev", debugValue="dev"
)
reprocess_all = dbutils.jobs.taskValues.get(
    taskKey=TASK_KEY, key="reprocess_all", default="False", debugValue="False"
)

dbutils.widgets.dropdown("brand", "acme", ["acme", "globex"])
brand_name = dbutils.widgets.get("brand")
secrets_scope = f"{ENV}-{brand_name}"

bucket_name = f"{ENV}-{brand_name}-{PRODUCT_NAME}-data"
data_path_prefix = "s3://" + "/".join([bucket_name, PROJECT_NAME, PRODUCT_NAME, brand_name])
print("Data path prefix:", data_path_prefix)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storage Credentials

# COMMAND ----------

ACCESS_KEY = dbutils.secrets.get(scope=secrets_scope, key="s3_access_key")
SECRET_KEY = dbutils.secrets.get(scope=secrets_scope, key="s3_access_secret")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", SECRET_KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Database Connections

# COMMAND ----------

db_host = dbutils.secrets.get(scope=secrets_scope, key="db_host")
db_user = dbutils.secrets.get(scope=secrets_scope, key="db_user")
db_password = dbutils.secrets.get(scope=secrets_scope, key="db_password")
db_name = f"{PRODUCT_NAME}_{brand_name}_{ENV}"
db_port = 5432

db_jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
db_connection_props = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver",
}

sf_user = json.loads(dbutils.secrets.get(scope=secrets_scope, key="snowflake_reader"))["user"]
sf_account = json.loads(dbutils.secrets.get(scope=secrets_scope, key="snowflake_reader"))["account"]
sf_url = json.loads(dbutils.secrets.get(scope=secrets_scope, key="snowflake_reader"))["url"]
sf_private_key = json.loads(dbutils.secrets.get(scope=secrets_scope, key="snowflake_reader"))["privateKey"]

sf_options = {
    "sfURL": sf_url,
    "sfAccount": sf_account,
    "sfUser": sf_user,
    "pem_private_key": sf_private_key,
}

print("Credentials loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Range Configuration

# COMMAND ----------

default_start_date = "20230101"
default_end_date = datetime.now().date().strftime("%Y%m%d")

start_date = dbutils.jobs.taskValues.get(
    taskKey=TASK_KEY, key="START_DATE", default=default_start_date, debugValue=default_start_date
)
end_date = dbutils.jobs.taskValues.get(
    taskKey=TASK_KEY, key="END_DATE", default=default_end_date, debugValue=default_end_date
)

if reprocess_all == "True":
    reprocess_all = True
else:
    reprocess_all = False
print("reprocess_all:", reprocess_all)

lookback_days = 30
print("lookback_days:", lookback_days)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable Utilities

# COMMAND ----------

class DataSyncHelper:
    """Reusable helper for syncing DataFrames to an external database."""

    def __init__(self, jdbc_url, connection_props, s3_prefix):
        self.jdbc_url = jdbc_url
        self.connection_props = connection_props
        self.s3_prefix = s3_prefix

    def overwrite_table(self, dataframe, table_name, repartition=10):
        """Write a DataFrame to a target table, overwriting existing data."""
        dataframe.repartition(repartition).write.jdbc(
            url=self.jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=self.connection_props,
        )

    def read_table(self, query):
        """Read data from the database using a query."""
        return spark.read.jdbc(url=self.jdbc_url, table=query, properties=self.connection_props)

    def save_to_s3(self, table_name, dataframe, time_index=None, update_start=None, reprocess=False):
        """Persist a DataFrame to S3 in parquet format."""
        output_path = f"{self.s3_prefix}/{table_name}"
        if reprocess:
            dataframe.write.mode("overwrite").parquet(output_path)
        else:
            dataframe.write.mode("append").parquet(output_path)


def get_update_window(end_date_str, lookback_days):
    """Calculate the start date for incremental updates."""
    end_dt = datetime.strptime(end_date_str, "%Y%m%d")
    start_dt = end_dt - timedelta(days=lookback_days)
    return start_dt.strftime("%Y%m%d"), start_dt.strftime("%Y-%m")
