"""
Reusable DataFrame transformations.
"""
from pyspark.sql.functions import col, trim, lower, current_timestamp, lit


def clean_string_columns(df, columns):
    """Trim and lowercase string columns."""
    for column in columns:
        df = df.withColumn(column, trim(lower(col(column))))
    return df


def add_audit_columns(df):
    """Add standard audit columns to a DataFrame."""
    return df \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("updated_at", current_timestamp()) \
        .withColumn("created_by", lit("etl_pipeline"))


def filter_nulls(df, key_columns):
    """Filter out rows where any key column is null."""
    condition = None
    for column in key_columns:
        if condition is None:
            condition = col(column).isNotNull()
        else:
            condition = condition & col(column).isNotNull()
    return df.filter(condition)
