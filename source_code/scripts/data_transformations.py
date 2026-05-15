"""
Functional transformation utilities for PySpark DataFrames.

Provides reusable, composable transformation functions designed to work
with the DataFrame.transform() API, as well as standalone helpers for:

- Column name normalisation
- Null value handling
- Schema coercion
- Conditional value bucketing (age, recency, time period)
- Pandas interop for date-range gap filling
"""

import re
import datetime

import pandas as pd
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, lit, datediff, current_date,
    month, year, concat, date_trunc, date_format,
    to_date, trim,
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, DecimalType


# ---------------------------------------------------------------------------
# Column normalisation
# ---------------------------------------------------------------------------

def normalise_column_names(df: DataFrame) -> DataFrame:
    """
    Lowercase all column names and replace non-alphanumeric characters
    (spaces, parentheses, slashes, etc.) with underscores.

    Example: "Total Sales (USD)" → "total_sales_usd"
    """
    rename_map = {
        c: re.sub(r"[^0-9a-zA-Z]+", "_", c).strip("_").lower()
        for c in df.columns
    }
    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)
    return df


def rename_columns_with_suffix(suffix: str):
    """
    Append *suffix* to every column name.
    Intended for use with DataFrame.transform().
    """
    def _(df: DataFrame) -> DataFrame:
        for c in df.columns:
            df = df.withColumnRenamed(c, c + suffix)
        return df
    return _


def lowercase_column_names(df: DataFrame) -> DataFrame:
    """Convert all column names to lower-case."""
    return df.toDF(*[c.lower() for c in df.columns])


# ---------------------------------------------------------------------------
# Null handling
# ---------------------------------------------------------------------------

def fill_string_nulls(fill_value: str = "-"):
    """
    Replace nulls in all StringType columns with *fill_value*.
    Intended for use with DataFrame.transform().
    """
    def _(df: DataFrame) -> DataFrame:
        string_cols = [
            f.name for f in df.schema.fields
            if isinstance(f.dataType, StringType)
        ]
        return df.na.fill(fill_value, subset=string_cols)
    return _


def fill_numeric_nulls(fill_value: float = 0.0):
    """
    Replace nulls in all numeric columns with *fill_value*.
    Intended for use with DataFrame.transform().
    """
    def _(df: DataFrame) -> DataFrame:
        numeric_types = {"IntegerType", "LongType", "DoubleType", "FloatType"}
        numeric_cols = [
            f.name for f in df.schema.fields
            if type(f.dataType).__name__ in numeric_types
            or isinstance(f.dataType, DecimalType)
        ]
        return df.na.fill(fill_value, subset=numeric_cols)
    return _


def coalesce_null(column_name: str, default_value):
    """
    Replace nulls in *column_name* with *default_value*.
    Intended for use with DataFrame.transform().
    """
    def _(df: DataFrame) -> DataFrame:
        return df.withColumn(
            column_name,
            when(col(column_name).isNull(), lit(default_value))
            .otherwise(col(column_name)),
        )
    return _


# ---------------------------------------------------------------------------
# Schema coercion
# ---------------------------------------------------------------------------

def impose_column_types(type_map: dict):
    """
    Cast named columns to specified PySpark types.

    Parameters
    ----------
    type_map : dict
        Maps column name → PySpark DataType, e.g.:
        {"price": DoubleType(), "quantity": IntegerType()}

    Intended for use with DataFrame.transform().
    """
    def _(df: DataFrame) -> DataFrame:
        for column_name, dtype in type_map.items():
            if column_name in df.columns:
                df = df.withColumn(column_name, col(column_name).cast(dtype))
        return df
    return _


def truncate_timestamps(timestamp_columns: list, precision: str = "second"):
    """
    Truncate timestamp columns to *precision* (e.g. 'second', 'minute', 'hour').
    Intended for use with DataFrame.transform().
    """
    def _(df: DataFrame) -> DataFrame:
        for c in timestamp_columns:
            if c in df.columns:
                df = df.withColumn(c, date_trunc(precision, col(c)))
        return df
    return _


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Strip leading/trailing whitespace from all StringType columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    return df


# ---------------------------------------------------------------------------
# Conditional bucketing
# ---------------------------------------------------------------------------

def with_age_bucket(age_column: str = "age", output_column: str = "age_bucket"):
    """
    Classify *age_column* into standard demographic age buckets.
    Intended for use with DataFrame.transform().
    """
    def _(df: DataFrame) -> DataFrame:
        return df.withColumn(
            output_column,
            when(col(age_column) < 18,  "Under 18")
            .when((col(age_column) >= 18)  & (col(age_column) < 25), "18-24")
            .when((col(age_column) >= 25)  & (col(age_column) < 35), "25-34")
            .when((col(age_column) >= 35)  & (col(age_column) < 45), "35-44")
            .when((col(age_column) >= 45)  & (col(age_column) < 55), "45-54")
            .when((col(age_column) >= 55)  & (col(age_column) < 65), "55-64")
            .otherwise("65+"),
        )
    return _


def with_recency_bucket(date_column: str, output_column: str = "recency_bucket"):
    """
    Compute days since *date_column* relative to today and assign a
    recency tier label.
    Intended for use with DataFrame.transform().
    """
    def _(df: DataFrame) -> DataFrame:
        days = datediff(current_date(), to_date(col(date_column)))
        return df.withColumn(
            output_column,
            when(days <= 7,   "Last 7 days")
            .when(days <= 30,  "Last 30 days")
            .when(days <= 90,  "Last 90 days")
            .when(days <= 365, "Last year")
            .otherwise("Older"),
        )
    return _


def with_time_period(date_column: str, output_column: str = "time_period"):
    """
    Map a date column to a calendar-quarter label (e.g. '2024 Q2').
      Jan–Mar → Q1 | Apr–Jun → Q2 | Jul–Sep → Q3 | Oct–Dec → Q4
    Intended for use with DataFrame.transform().
    """
    def _(df: DataFrame) -> DataFrame:
        m = month(col(date_column))
        y = year(col(date_column)).cast(StringType())
        return df.withColumn(
            output_column,
            when(m <= 3,  concat(y, lit(" Q1")))
            .when(m <= 6,  concat(y, lit(" Q2")))
            .when(m <= 9,  concat(y, lit(" Q3")))
            .otherwise(concat(y, lit(" Q4"))),
        )
    return _


def with_value_label(column_name: str, mapping: list, output_column: str = None):
    """
    Map discrete values in *column_name* to human-readable labels using
    a list of (raw_value, label) tuples.

    Parameters
    ----------
    column_name   : Source column to inspect
    mapping       : [(raw_value, label), ...]  — evaluated in order
    output_column : Destination column (defaults to column_name + "_label")

    Intended for use with DataFrame.transform().
    """
    out_col = output_column or (column_name + "_label")

    def _(df: DataFrame) -> DataFrame:
        if not mapping:
            return df.withColumn(out_col, lit(None).cast(StringType()))
        condition = when(col(column_name) == mapping[0][0], mapping[0][1])
        for raw, label in mapping[1:]:
            condition = condition.when(col(column_name) == raw, label)
        condition = condition.otherwise(lit(None).cast(StringType()))
        return df.withColumn(out_col, condition)
    return _


# ---------------------------------------------------------------------------
# Pandas interop — date-range gap filling
# ---------------------------------------------------------------------------

def fill_missing_dates(
    spark: SparkSession,
    df: DataFrame,
    date_column: str,
    group_column: str,
    value_columns: list,
) -> DataFrame:
    """
    Forward-fill gaps in a daily time series using pandas.

    For each group in *group_column*, expands the date range from the
    group's minimum date to today and forward-fills *value_columns*.

    Parameters
    ----------
    spark         : Active SparkSession
    df            : Source Spark DataFrame
    date_column   : Name of the date column
    group_column  : Column to group by before filling
    value_columns : Numeric columns to forward-fill

    Returns
    -------
    Spark DataFrame with gaps filled
    """
    pdf = df.toPandas()
    pdf[date_column] = pd.to_datetime(pdf[date_column])
    groups = []

    for key, group in pdf.groupby(group_column):
        full_range = pd.date_range(
            start=group[date_column].min(),
            end=datetime.date.today(),
        )
        group = (
            group.set_index(date_column)
            .reindex(full_range)
            .rename_axis(date_column)
            .reset_index()
        )
        group[group_column] = key
        for vc in value_columns:
            if vc in group.columns:
                group[vc] = group[vc].ffill()
        groups.append(group)

    filled_pdf = pd.concat(groups, ignore_index=True)
    return spark.createDataFrame(filled_pdf)


# ---------------------------------------------------------------------------
# Self-contained demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("DataTransformationsDemo")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_data = [
        (1, "Alice",   29, "2024-11-01", 1200.50, "Electronics"),
        (2, "Bob",     45, "2023-06-15",  340.00, "Clothing"),
        (3, "Carol",   17, "2024-01-20",   89.99, "Electronics"),
        (4, "Dave",    62, None,          None,   "Home Goods"),
        (5, "Eve",     38, "2022-03-08",  720.00, "Clothing"),
        (6, "Frank",   None, "2024-09-12", 55.00, None),
    ]
    columns = [
        "customer_id", "Customer Name",
        "age", "last_purchase_date",
        "spend (USD)", "category",
    ]
    df = spark.createDataFrame(raw_data, columns)

    result = (
        df
        .transform(normalise_column_names)
        .transform(fill_string_nulls("-"))
        .transform(fill_numeric_nulls(0.0))
        .transform(trim_string_columns)
        .transform(with_age_bucket("age", "age_bucket"))
        .transform(with_recency_bucket("last_purchase_date", "recency_bucket"))
        .transform(with_time_period("last_purchase_date", "purchase_period"))
        .transform(with_value_label(
            "category",
            [("Electronics", "Tech"), ("Clothing", "Apparel"),
             ("Home Goods", "Home")],
            "category_label",
        ))
    )

    print("=== Transformed DataFrame ===")
    result.show(truncate=False)
    result.printSchema()

    # -- Pandas gap-fill demo -------------------------------------------
    series_data = [
        ("2024-01-01", "A", 100.0),
        ("2024-01-03", "A", 110.0),
        ("2024-01-01", "B",  50.0),
        ("2024-01-04", "B",  55.0),
    ]
    series_df = spark.createDataFrame(series_data, ["date", "group", "value"])
    filled_df = fill_missing_dates(spark, series_df, "date", "group", ["value"])
    print("=== Gap-filled time series (first 10 rows) ===")
    filled_df.orderBy("group", "date").show(10)

    spark.stop()
