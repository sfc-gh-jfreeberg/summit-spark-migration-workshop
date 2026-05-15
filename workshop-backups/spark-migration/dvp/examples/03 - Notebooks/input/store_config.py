# Databricks notebook source
# MAGIC %md
# MAGIC # Store Analytics Configuration
# MAGIC Shared configuration loaded by all store analytics notebooks via `%run`.
# MAGIC Sets up environment, credentials, date ranges, and reusable utilities.

# COMMAND ----------

import json
import os
import re
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
import calendar

print("Packages imported")

spark.conf.set("spark.sql.caseSensitive", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment & Widget Parameters

# COMMAND ----------

PROD = "prod"
DEV = "dev"
STG = "staging"

default_end_date = datetime.now().date().strftime("%Y%m%d")

dbutils.widgets.dropdown("environment", DEV, [DEV, PROD, STG], "Environment")
dbutils.widgets.dropdown("reprocess_all", "False", ["True", "False"], "Reprocess All Data")
dbutils.widgets.text("start_date", "20220101", "Start Date")
dbutils.widgets.text("end_date", default_end_date, "End Date")
dbutils.widgets.text("lookback_days", "10", "Lookback Days")

# COMMAND ----------

TASK_KEY = "store_analytics"
PROJECT_NAME = "retail_ops"
PRODUCT_NAME = "store_kpi"

reprocess_all = dbutils.widgets.get("reprocess_all")
dbutils.jobs.taskValues.set(key="reprocess_all", value=reprocess_all)

ENV = dbutils.widgets.get("environment")
dbutils.jobs.taskValues.set(key="ENV", value=ENV)
dbutils.jobs.taskValues.set(key="PROD", value=PROD)
dbutils.jobs.taskValues.set(key="DEV", value=DEV)

start_date = dbutils.widgets.get("start_date")
dbutils.jobs.taskValues.set(key="START_DATE", value=start_date)

end_date = dbutils.widgets.get("end_date")
dbutils.jobs.taskValues.set(key="END_DATE", value=end_date)

lookback_days = int(dbutils.widgets.get("lookback_days"))

dbutils.widgets.dropdown("region", "NA", ["NA", "EMEA", "APAC"], "Region")
region = dbutils.widgets.get("region")

if reprocess_all == "True":
    reprocess_all = True
else:
    reprocess_all = False

print("reprocess_all:", reprocess_all)
print("ENV:", ENV)
print("start_date:", start_date)
print("end_date:", end_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Credentials & Connection Setup

# COMMAND ----------

env_nm = f"{PROJECT_NAME}-{ENV}"
product_nm = PRODUCT_NAME
secrets_scope = f"{ENV}-{region.lower()}"
bucket_nm = f"{ENV}-{region.lower()}-{PROJECT_NAME}-data"
path_prefix = "s3://" + "/".join([bucket_nm, env_nm, product_nm, region.lower()])
print("Set path_prefix:", path_prefix)

db_user = dbutils.secrets.get(scope=secrets_scope, key="db_user")
db_password = dbutils.secrets.get(scope=secrets_scope, key="db_password")
db_host = dbutils.secrets.get(scope=secrets_scope, key="db_host")

db_name = f"prod_warehouse_{region.lower()}"
jdbc_url = f"jdbc:postgresql://{db_host}:5432/{db_name}"
connection_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}
print("Database connection configured:", db_name)

ACCESS_KEY = dbutils.secrets.get(scope=secrets_scope, key="s3_access_key")
SECRET_KEY = dbutils.secrets.get(scope=secrets_scope, key="s3_secret_key")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", SECRET_KEY)

print("Credentials loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snowflake Writer Configuration

# COMMAND ----------

sf_writer_config = json.loads(dbutils.secrets.get(scope=secrets_scope, key="snowflake_writer"))
sf_writer_user = sf_writer_config["user"]
sf_writer_account = sf_writer_config["account"]
sf_writer_database = sf_writer_config["databaseName"]
sf_writer_warehouse = sf_writer_config["warehouse"]
sf_writer_role = sf_writer_config["role"]
sf_writer_schema = sf_writer_config["schema"]
sf_writer_private_key = sf_writer_config["privateKey"]

pkb = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n", "", sf_writer_private_key).replace("\n", "")

sf_writer_options = {
    "sfURL": sf_writer_config["url"],
    "sfAccount": sf_writer_account,
    "sfUser": sf_writer_user,
    "sfRole": sf_writer_role,
    "pem_private_key": pkb,
    "sfDatabase": sf_writer_database,
    "sfSchema": sf_writer_schema,
    "sfWarehouse": sf_writer_warehouse,
}

print("Snowflake writer configured:", sf_writer_database)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Range Configuration

# COMMAND ----------

default_start_time = "20220101"

start_date = dbutils.jobs.taskValues.get(
    taskKey=TASK_KEY, key="START_DATE",
    default=default_start_time, debugValue=default_start_time
)
end_date = dbutils.jobs.taskValues.get(
    taskKey=TASK_KEY, key="END_DATE",
    default=default_end_date, debugValue=default_end_date
)

print("start_date:", start_date)
print("end_date:", end_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Utility Functions

# COMMAND ----------

def get_update_window(end_dt, days_back):
    """Compute update start date and month from end date and lookback."""
    end = datetime.strptime(end_dt, "%Y%m%d")
    start = end - timedelta(days=days_back)
    return start.strftime("%Y%m%d"), start.strftime("%Y-%m")


def year_month_to_quarter(year_month):
    """Convert yyyy-MM to quarter label like '2024 Q3'."""
    dt = datetime.strptime(year_month, "%Y-%m")
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year} Q{q}"


import snowflake.connector

def snowflake_update(table_name, time_index, update_start_time, schema=None):
    """Write updated data to Snowflake for a given table."""
    print(f"  Updating {table_name} from {update_start_time}...")
    with snowflake.connector.connect(
        account=sf_writer_account,
        warehouse=sf_writer_warehouse,
        role=sf_writer_role,
        database=sf_writer_database,
        user=sf_writer_user,
        schema=sf_writer_schema,
    ) as conn:
        cur = conn.cursor()
        if update_start_time:
            cur.execute(
                f"DELETE FROM {table_name} WHERE {time_index} >= '{update_start_time}'"
            )
        cur.execute(f"INSERT INTO {table_name} SELECT * FROM @staging/{table_name}")
    print(f"  Finished {table_name}.")
