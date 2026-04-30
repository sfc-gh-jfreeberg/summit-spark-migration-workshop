"""
Pipeline helper class for managing PySpark data transformations.

Provides utilities for:
- Unioning DataFrames from multiple sources
- Generating SCD audit columns (business key hash, Type I / Type II hashes)
- Enforcing schemas and filtering data vs. audit columns
- DataFrame caching
"""

import datetime
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, sha2, concat_ws, lit, current_timestamp, when, substring
)
from pyspark.sql.types import (
    StringType, BinaryType, TimestampType, BooleanType
)


class PipelineHelper:
    """
    Helper class for common data pipeline operations.

    Parameters
    ----------
    business_key_columns : list[str]
        Column names that form the natural/business key of the entity.
    type_i_columns_list : list[str], optional
        Columns that trigger a Type I (in-place) update when changed.
        All other non-key columns are treated as Type II (versioned).
    creation_dt_column : str, optional
        Source column representing record creation time.
    modification_dt_column : str, optional
        Source column representing record last-modified time.
    """

    def __init__(
        self,
        business_key_columns: list,
        type_i_columns_list: list = None,
        creation_dt_column: str = "",
        modification_dt_column: str = "",
    ):
        self._business_key_columns = business_key_columns
        self._type_i_columns_list = type_i_columns_list or []
        self.creation_dt_column = creation_dt_column
        self.modification_dt_column = modification_dt_column
        self._process_datetime = datetime.datetime.now()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _sorted_columns(self, columns: list) -> list:
        """Return *columns* sorted case-insensitively."""
        return sorted(columns, key=lambda c: c.lower())

    def _hash_columns(self, columns: list):
        """SHA-256 hash of the sorted, pipe-delimited column values."""
        sorted_cols = self._sorted_columns(columns)
        return sha2(
            concat_ws("|", *[col(c).cast(StringType()) for c in sorted_cols]),
            256,
        ).cast(BinaryType())

    def _get_data_columns(self, df: DataFrame) -> list:
        """Column names that do NOT start with '__' (non-audit columns)."""
        return [c for c in df.columns if not c.startswith("__")]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def union_dataframes(self, df_list: list) -> DataFrame:
        """
        Union a list of DataFrames by name (column-order-independent).

        Parameters
        ----------
        df_list : list[DataFrame]
            All DataFrames must share an identical schema.

        Returns
        -------
        DataFrame
        """
        return reduce(DataFrame.unionByName, df_list)

    def select_data_columns(self, df: DataFrame) -> DataFrame:
        """Return *df* with all audit columns (names starting with '__') dropped."""
        return df.select(*self._get_data_columns(df))

    def cache_dataframe(self, df: DataFrame) -> DataFrame:
        """Cache *df* and trigger an action to materialise the cache."""
        df.cache()
        count = df.count()
        print(f"Cached {count:,} records.")
        return df

    def add_dimension_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Add SCD audit columns to a dimension DataFrame.

        Added columns
        -------------
        __BusinessKeyHash          : SHA-256 of the business key columns
        __Type1Hash                : SHA-256 of Type I (in-place) columns
        __Type2Hash                : SHA-256 of remaining non-key data columns
        __DeletedFlag              : False for active records
        __CreateDateTime           : Pipeline run timestamp
        __DataCreationDateTime     : Source creation timestamp (or NULL)
        __DataModificationDateTime : Source modification timestamp (or NULL)
        """
        audit_cols = {self.creation_dt_column, self.modification_dt_column}
        non_type_i = list(
            set(self._get_data_columns(df))
            - set(self._type_i_columns_list)
            - set(self._business_key_columns)
            - audit_cols
        )

        result = df.withColumn(
            "__BusinessKeyHash", self._hash_columns(self._business_key_columns)
        )

        if self._type_i_columns_list:
            result = result.withColumn(
                "__Type1Hash", self._hash_columns(self._type_i_columns_list)
            )
        else:
            result = result.withColumn("__Type1Hash", lit(None).cast(BinaryType()))

        if non_type_i:
            result = result.withColumn(
                "__Type2Hash", self._hash_columns(non_type_i)
            )
        else:
            result = result.withColumn("__Type2Hash", lit(None).cast(BinaryType()))

        result = (
            result
            .withColumn("__DeletedFlag", lit(False).cast(BooleanType()))
            .withColumn(
                "__CreateDateTime",
                lit(self._process_datetime).cast(TimestampType()),
            )
        )

        if self.creation_dt_column and self.creation_dt_column in df.columns:
            result = result.withColumn(
                "__DataCreationDateTime",
                when(
                    col(self.creation_dt_column).isNotNull(),
                    col(self.creation_dt_column).cast(TimestampType()),
                ).otherwise(lit(None).cast(TimestampType())),
            )
        else:
            result = result.withColumn(
                "__DataCreationDateTime", lit(None).cast(TimestampType())
            )

        if self.modification_dt_column and self.modification_dt_column in df.columns:
            result = result.withColumn(
                "__DataModificationDateTime",
                when(
                    col(self.modification_dt_column).isNotNull(),
                    col(self.modification_dt_column).cast(TimestampType()),
                ).otherwise(lit(None).cast(TimestampType())),
            )
        else:
            result = result.withColumn(
                "__DataModificationDateTime", lit(None).cast(TimestampType())
            )

        return result

    def add_fact_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Add audit columns to a fact DataFrame.

        Added columns
        -------------
        __FactKeyHash    : SHA-256 of business key columns
        __DeletedFlag    : False for active records
        __CreateDateTime : Pipeline run timestamp
        """
        return (
            df
            .withColumn("__FactKeyHash", self._hash_columns(self._business_key_columns))
            .withColumn("__DeletedFlag", lit(False).cast(BooleanType()))
            .withColumn(
                "__CreateDateTime",
                lit(self._process_datetime).cast(TimestampType()),
            )
        )

    def impose_schema(self, df: DataFrame, schema) -> DataFrame:
        """
        Cast each non-audit column to the type declared in *schema*.

        Parameters
        ----------
        df     : Source DataFrame
        schema : pyspark.sql.types.StructType

        Returns
        -------
        DataFrame with all declared columns cast to the target type.
        """
        for field in schema.fields:
            if not field.name.startswith("__") and field.name in df.columns:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        return df

    def find_partition_keys(self, df: DataFrame, id_columns: list) -> dict:
        """
        Extract the leading 4-character prefix of each ID column for
        partition pruning on right-side join inputs.

        Parameters
        ----------
        df         : Left-side DataFrame
        id_columns : ID columns to inspect

        Returns
        -------
        dict mapping column name → list of distinct 4-char prefixes
        """
        result = {}
        for c in id_columns:
            result[c] = [
                row["prefix"]
                for row in df
                .select(substring(col(c).cast(StringType()), 1, 4).alias("prefix"))
                .where(col("prefix").isNotNull())
                .distinct()
                .collect()
            ]
        return result


# ---------------------------------------------------------------------------
# Self-contained demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("PipelineHelperDemo")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # -- Dimension example -------------------------------------------------
    dim_data = [
        (1, "Widget A", "Electronics", "2024-01-15", "2024-03-01"),
        (2, "Widget B", "Clothing",    "2024-02-20", "2024-02-20"),
        (3, "Widget C", "Electronics", "2023-11-05", "2024-01-10"),
    ]
    dim_columns = ["product_id", "product_name", "category", "created_at", "updated_at"]
    dim_df = spark.createDataFrame(dim_data, dim_columns)

    helper = PipelineHelper(
        business_key_columns=["product_id"],
        type_i_columns_list=["category"],
        creation_dt_column="created_at",
        modification_dt_column="updated_at",
    )

    enriched_dim = dim_df.transform(helper.add_dimension_audit_columns)
    print("=== Dimension with Audit Columns ===")
    enriched_dim.select(
        "product_id", "product_name", "__DeletedFlag", "__CreateDateTime"
    ).show(truncate=False)

    # -- Fact example ------------------------------------------------------
    fact_data = [
        (101, 1, "2024-03-01", 5, 49.95),
        (102, 2, "2024-03-02", 2, 29.99),
        (103, 1, "2024-03-03", 1, 9.99),
    ]
    fact_columns = ["order_id", "product_id", "order_date", "quantity", "revenue"]
    fact_df = spark.createDataFrame(fact_data, fact_columns)

    fact_helper = PipelineHelper(business_key_columns=["order_id"])
    enriched_fact = fact_df.transform(fact_helper.add_fact_audit_columns)
    print("=== Fact with Audit Columns ===")
    enriched_fact.select(
        "order_id", "product_id", "revenue", "__FactKeyHash", "__DeletedFlag"
    ).show(truncate=False)

    # -- Union example -----------------------------------------------------
    df_a = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "val"])
    df_b = spark.createDataFrame([(3, "C"), (4, "D")], ["id", "val"])
    df_c = spark.createDataFrame([(5, "E")],           ["id", "val"])

    unioned = helper.union_dataframes([df_a, df_b, df_c])
    print("=== Unioned DataFrame ===")
    unioned.show()

    spark.stop()
