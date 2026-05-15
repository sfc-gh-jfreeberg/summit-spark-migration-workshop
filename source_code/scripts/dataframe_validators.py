"""
DataFrame validation utilities for PySpark pipelines.

Provides functions to assert column presence, value ranges, schema
conformance, and row-level data quality — raising descriptive errors
on failure rather than silently producing incorrect results downstream.
"""

import logging
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class DataFrameValidationError(Exception):
    """Base class for all DataFrame validation failures."""


class MissingColumnsError(DataFrameValidationError):
    """Raised when expected columns are absent from a DataFrame."""


class ExtraColumnsError(DataFrameValidationError):
    """Raised when unexpected columns are present in a DataFrame."""


class ValueOutOfRangeError(DataFrameValidationError):
    """Raised when column values fall outside an expected numeric range."""


class ValueMissingError(DataFrameValidationError):
    """Raised when required values are null or zero."""


class DuplicateRowsError(DataFrameValidationError):
    """Raised when key columns are not unique across the DataFrame."""


class SchemaConformanceError(DataFrameValidationError):
    """Raised when StructFields do not match a required schema."""


# ---------------------------------------------------------------------------
# Column presence
# ---------------------------------------------------------------------------

def validate_columns_present(df: DataFrame, required: list) -> None:
    """
    Assert that all *required* columns exist in *df*.

    Parameters
    ----------
    df       : DataFrame to inspect
    required : Column names that must be present

    Raises
    ------
    MissingColumnsError
    """
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise MissingColumnsError(
            f"Columns missing from DataFrame: {missing}. "
            f"Available columns: {df.columns}"
        )


def validate_exact_columns(df: DataFrame, expected: list) -> None:
    """
    Assert that *df* has exactly the *expected* columns — no more, no less.

    Parameters
    ----------
    df       : DataFrame to inspect
    expected : Exact column names expected

    Raises
    ------
    MissingColumnsError, ExtraColumnsError
    """
    validate_columns_present(df, expected)
    extra = [c for c in df.columns if c not in expected]
    if extra:
        raise ExtraColumnsError(
            f"Unexpected columns in DataFrame: {extra}. "
            f"Expected exactly: {expected}"
        )


def validate_columns_absent(df: DataFrame, prohibited: list) -> None:
    """
    Assert that none of the *prohibited* columns appear in *df*.

    Raises
    ------
    ExtraColumnsError
    """
    found = [c for c in df.columns if c in prohibited]
    if found:
        raise ExtraColumnsError(
            f"Prohibited columns found in DataFrame: {found}"
        )


# ---------------------------------------------------------------------------
# Value ranges and nulls
# ---------------------------------------------------------------------------

def validate_value_range(
    df: DataFrame,
    column: str,
    lower: float,
    upper: float,
) -> None:
    """
    Assert that all values in *column* fall within [lower, upper].

    Raises
    ------
    ValueOutOfRangeError
    """
    validate_columns_present(df, [column])
    bad_rows = df.filter(~F.col(column).between(lower, upper)).take(1)
    if bad_rows:
        raise ValueOutOfRangeError(
            f"Column '{column}' has values outside [{lower}, {upper}]. "
            f"Example row: {bad_rows[0]}"
        )


def validate_no_nulls(df: DataFrame, columns: list) -> None:
    """
    Assert that none of *columns* contain null values.

    Raises
    ------
    ValueMissingError
    """
    validate_columns_present(df, columns)
    for c in columns:
        null_count = df.filter(F.col(c).isNull()).count()
        if null_count > 0:
            raise ValueMissingError(
                f"Column '{c}' contains {null_count:,} null value(s)."
            )


def validate_non_zero(df: DataFrame, columns: list) -> None:
    """
    Assert that none of *columns* contain zero values.

    Raises
    ------
    ValueMissingError
    """
    validate_columns_present(df, columns)
    for c in columns:
        zero_count = df.filter(F.col(c) == 0).count()
        if zero_count > 0:
            raise ValueMissingError(
                f"Column '{c}' contains {zero_count:,} zero value(s)."
            )


def validate_allowed_values(
    df: DataFrame,
    column: str,
    allowed: list,
) -> None:
    """
    Assert that every non-null value in *column* belongs to *allowed*.

    Raises
    ------
    ValueOutOfRangeError
    """
    validate_columns_present(df, [column])
    bad_rows = (
        df
        .filter(F.col(column).isNotNull())
        .filter(~F.col(column).isin(allowed))
        .take(1)
    )
    if bad_rows:
        raise ValueOutOfRangeError(
            f"Column '{column}' contains values not in allowed set {allowed}. "
            f"Example row: {bad_rows[0]}"
        )


def validate_rate_within_range(
    df: DataFrame,
    column: str,
    lower: float = 0.0,
    upper: float = 100.0,
) -> None:
    """
    Convenience wrapper for rate/percentage columns — asserts [lower, upper]
    and also checks for nulls.

    Raises
    ------
    ValueMissingError, ValueOutOfRangeError
    """
    validate_no_nulls(df, [column])
    validate_value_range(df, column, lower, upper)


# ---------------------------------------------------------------------------
# Uniqueness
# ---------------------------------------------------------------------------

def validate_no_duplicates(df: DataFrame, key_columns: list) -> None:
    """
    Assert that the combination of *key_columns* is unique across *df*.

    Raises
    ------
    DuplicateRowsError
    """
    validate_columns_present(df, key_columns)
    total = df.count()
    distinct = df.select(*key_columns).distinct().count()
    if total != distinct:
        raise DuplicateRowsError(
            f"Duplicate rows found on key columns {key_columns}: "
            f"{total:,} total rows but only {distinct:,} distinct key combinations."
        )


# ---------------------------------------------------------------------------
# Schema conformance
# ---------------------------------------------------------------------------

def validate_schema(df: DataFrame, required_schema) -> None:
    """
    Assert that *df* contains every StructField in *required_schema*,
    with matching data types.

    Parameters
    ----------
    df              : DataFrame to inspect
    required_schema : pyspark.sql.types.StructType

    Raises
    ------
    SchemaConformanceError
    """
    actual = {f.name: f.dataType for f in df.schema.fields}

    missing = [f for f in required_schema.fields if f.name not in actual]
    if missing:
        raise SchemaConformanceError(
            f"Missing StructFields: {[f.name for f in missing]}. "
            f"Actual schema fields: {list(actual.keys())}"
        )

    mismatches = [
        f for f in required_schema.fields
        if f.name in actual and actual[f.name] != f.dataType
    ]
    if mismatches:
        details = ", ".join(
            f"{f.name} (expected {f.dataType}, got {actual[f.name]})"
            for f in mismatches
        )
        raise SchemaConformanceError(f"Type mismatches: {details}")


# ---------------------------------------------------------------------------
# DataFrame equality (for unit testing)
# ---------------------------------------------------------------------------

def assert_dataframes_equal(
    df1: DataFrame,
    df2: DataFrame,
    ignore_columns: list = None,
) -> None:
    """
    Assert that two DataFrames contain identical rows (order-insensitive).

    Parameters
    ----------
    df1, df2        : DataFrames to compare
    ignore_columns  : Columns to exclude before comparison (e.g. timestamps)

    Raises
    ------
    DataFrameValidationError
    """
    if ignore_columns:
        df1 = df1.drop(*ignore_columns)
        df2 = df2.drop(*ignore_columns)

    rows1 = sorted(str(r) for r in df1.collect())
    rows2 = sorted(str(r) for r in df2.collect())

    if rows1 != rows2:
        raise DataFrameValidationError(
            f"DataFrames are not equal.\n"
            f"df1 columns : {df1.columns}\n"
            f"df2 columns : {df2.columns}\n"
            f"df1 row count: {len(rows1)}\n"
            f"df2 row count: {len(rows2)}"
        )


# ---------------------------------------------------------------------------
# Composite validator — run multiple checks at once
# ---------------------------------------------------------------------------

def run_validation_suite(df: DataFrame, checks: list) -> dict:
    """
    Execute a list of (label, callable) validation checks against *df*,
    collecting pass/fail results without aborting on the first failure.

    Parameters
    ----------
    df     : DataFrame to validate
    checks : List of (label: str, fn: Callable[[DataFrame], None]) tuples

    Returns
    -------
    dict with keys "passed", "failed", and "errors"
    """
    passed, failed, errors = [], [], {}
    for label, fn in checks:
        try:
            fn(df)
            passed.append(label)
            logger.info("PASS: %s", label)
        except DataFrameValidationError as exc:
            failed.append(label)
            errors[label] = str(exc)
            logger.warning("FAIL: %s — %s", label, exc)
    return {"passed": passed, "failed": failed, "errors": errors}


# ---------------------------------------------------------------------------
# Self-contained demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from pyspark.sql.types import (
        StructType, StructField,
        IntegerType, StringType, DoubleType,
    )

    spark = (
        SparkSession.builder
        .appName("ValidatorsDemo")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    data = [
        (1, "Alpha",  85.5, "active"),
        (2, "Beta",   92.0, "active"),
        (3, "Gamma",  78.3, "inactive"),
        (4, "Delta",  60.1, "active"),
    ]
    df = spark.createDataFrame(data, ["id", "name", "score", "status"])

    # -- Individual checks -------------------------------------------------
    required_schema = StructType([
        StructField("id",    IntegerType(), True),
        StructField("name",  StringType(),  True),
        StructField("score", DoubleType(),  True),
    ])

    checks = [
        ("columns present",         lambda d: validate_columns_present(d, ["id", "name", "score"])),
        ("score in range [0, 100]", lambda d: validate_value_range(d, "score", 0.0, 100.0)),
        ("no nulls on id/name",     lambda d: validate_no_nulls(d, ["id", "name"])),
        ("id is unique",            lambda d: validate_no_duplicates(d, ["id"])),
        ("schema conforms",         lambda d: validate_schema(d, required_schema)),
        ("status allowed values",   lambda d: validate_allowed_values(
            d, "status", ["active", "inactive", "pending"]
        )),
    ]

    results = run_validation_suite(df, checks)
    print(f"\nPassed: {results['passed']}")
    print(f"Failed: {results['failed']}")

    # -- Demonstrate a deliberate failure ----------------------------------
    bad_df = spark.createDataFrame(
        [(1, "X", 150.0, "unknown")],
        ["id", "name", "score", "status"],
    )
    try:
        validate_value_range(bad_df, "score", 0.0, 100.0)
    except ValueOutOfRangeError as e:
        print(f"\nCaught expected ValueOutOfRangeError:\n  {e}")

    try:
        validate_allowed_values(bad_df, "status", ["active", "inactive", "pending"])
    except ValueOutOfRangeError as e:
        print(f"\nCaught expected ValueOutOfRangeError (allowed values):\n  {e}")

    spark.stop()
