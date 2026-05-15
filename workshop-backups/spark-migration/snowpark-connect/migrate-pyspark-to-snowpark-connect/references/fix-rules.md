# SCOS Fix Rules Reference

Rules for fixing SCOS compatibility issues found during analysis. The fixer agent reads this document when applying fixes to migrated files.

**Related references:**
- `references/python/rdd-conversion.md` — RDD-to-DataFrame conversion rules and examples (required for Rule 2)
- `references/python/udf-dependencies.md` — UDF serialization tiered fix approach (required for Rules 10 and 11)
- `references/python/ewi-codes.md` — Official SCOS EWI code scheme (required for `# SCOS:` comment tagging)

---

## Pre-Fix: Read EWI Codes

Before applying fixes, read `references/python/ewi-codes.md` to understand the official SCOS EWI code scheme. When adding `# SCOS:` comments, include the relevant EWI code where possible. For example:
- `# SCOS: [SPRKCNTPY1500] RDD operation converted to DataFrame`
- `# SCOS: TODO - [SPRKCNTPY2500] ML element requires manual migration`

This tagging enables the report generator to map comments to official codes accurately.

---

## Per-Issue Processing

For EACH issue in `analysis.json`, perform the following:

1. **Locate the issue**: Find the code at `file` and `lines` in the copied directory.
2. **Assess the risk**: Check the `final_risk` value.
3. **Apply the appropriate action** based on the rules below.
4. **Document the action**: Next to the code chunk that you've just processed **ALWAYS** add a code comment explaining the potential issue root cause and explain the decision you have made. Add a comment regardless of whether you have decided to apply a fix or not — **except** for no-op operations and no-op configs, which should be left as-is without any comment (see General Rules 4 and 5). Use one of these prefixes so the validation skill can parse them:
   - `# SCOS: <explanation>` — fix applied or issue reviewed (no action needed)
   - `# SCOS: TODO - <explanation>` — requires manual review; could not be auto-fixed
   - `# SCOS: Performance tip - <explanation>` — optimization recommendation

---

## Rules for Fixing based on Risk Score

1. **Must Fix (`final_risk` >= 0.7)**: These are critical compatibility issues. You **MUST** apply a fix or rewrite the logic. If no direct fix is available, you must rewrite the code to avoid the unsupported feature. If a rewrite is not feasible, add `# SCOS: TODO - <explanation>` so the validation skill flags it.
2. **Should Fix (0.3 <= `final_risk` < 0.7)**: These are likely issues. You **SHOULD** apply a fix if one is suggested. If unsure, add `# SCOS: TODO - <explanation>` to flag it for manual review.
3. **Fix if possible (`final_risk` < 0.3)**: These are minor risks or potential false positives. You **MUST still review them** and apply a fix if possible. If the code is safe, just add a comment `# SCOS: <explanation>`.

---

## General Rules

1. **Use the Tool's Fix**: If the issue object provides a `fix` value, use it. It is tailored to the specific error.

2. **Handle RDDs**: RDD operations (`final_risk` near 1.0) are not supported. You MUST rewrite them using DataFrame transformations or SQL expressions. **Read** `references/python/rdd-conversion.md` for detailed conversion rules and examples.

3. **Unsupported Formats**: Change file formats if required (e.g., ORC/Avro -> Parquet).

4. **No-Op Operations**: Operations like `hint()`, `repartition()`, or `coalesce()` are silently ignored in SCOS — they have no effect but do not cause errors. Leave this code as-is without adding any comment. No code change or annotation is needed.

5. **No-Op Configs**: Spark configs that are not supported by SCOS (category: "No-Op Config") are silently ignored — they have no effect but do not cause errors. Leave this code as-is without adding any comment. No code change or annotation is needed. Common no-op configs include `spark.sql.shuffle.partitions`, `spark.executor.memory`, `spark.driver.memory`, `spark.sql.adaptive.enabled`, etc.

6. **Missing Fixes**: If `fix` is null, use the `root_cause` to determine the best workaround. If unsure, add a TODO comment: `# SCOS: TODO - <explanation>`.

7. **File Reads**: For file read operations (`.read.csv`, `.read.json`, `.read.parquet`, `.load`), check the path being read:
    - **Already using Snowflake stage** (`@STAGE_NAME/...` or `@~/...`): No comment needed, this is optimal.
    - **External cloud storage** (paths starting with `s3://`, `s3a://`, `gs://`, `abfs://`, `wasb://`, `adl://`): Add performance comment recommending Snowflake stage upload.
    - **Local paths or variables**: If the path is a variable, trace it to determine if it's external cloud storage. Add performance comment recommending Snowflake stage upload for both.

    ```python
    # SCOS: Performance tip - Consider uploading this file to a Snowflake stage
    # for faster processing. Use: session.file.put("local_path", "@STAGE_NAME/path")
    df = spark.read.csv("s3://bucket/path/file.csv", header=True)
    ```

8. **Snowflake Connector Pushdown (Recommended)**: If code uses the Spark Snowflake Connector (`.format("snowflake")` with `.options(...)` and `.load()`), recommend replacing it with `SnowflakeSession.sql()`. The connector is **supported and functional** in SCOS, but `SnowflakeSession` provides a better experience: simpler code, no connector config boilerplate, and direct use of the Snowpark Connect session. Since this is a recommendation (not a required fix), add a comment with the complete suggested replacement code while keeping the original code intact.

    **BEFORE:**
    ```python
    rest_data_info = spark.read \
       .format("snowflake") \
       .options(**sfOptions) \
       .option("sfDatabase", "BRAND_PLK") \
       .option("sfSchema", "STORES") \
       .option("sfWarehouse", "ANALYSIS_PLK") \
       .option("query", f"""
           select store_id as rest_no, full_address as rest_address
           from STORES where status = 'OPEN'
       """) \
       .load()
    ```

    **Comment with suggested replacement:**
    ```python
    # SCOS: Recommended improvement - The Snowflake Connector (.format("snowflake")) works
    # in SCOS but SnowflakeSession.sql() provides a better experience. Suggested replacement:
    #
    #   from snowflake.snowpark_connect.snowflake_session import SnowflakeSession
    #   snowflake_session = SnowflakeSession(spark)
    #   snowflake_session.sql("USE DATABASE BRAND_PLK").collect()
    #   snowflake_session.sql("USE SCHEMA STORES").collect()
    #   snowflake_session.sql("USE WAREHOUSE ANALYSIS_PLK").collect()
    #   rest_data_info = snowflake_session.sql("""
    #       select store_id as rest_no, full_address as rest_address
    #       from STORES where status = 'OPEN'
    #   """)
    rest_data_info = spark.read \
       .format("snowflake") \
       .options(**sfOptions) \
       ...
       .load()
    ```

    **Key mapping rules for the suggestion:**
    - Extract the SQL from `.option("query", ...)` and pass it to `snowflake_session.sql()`
    - If `.option("dbtable", "TABLE_NAME")` is used instead of `query`, suggest `snowflake_session.sql("SELECT * FROM TABLE_NAME")`
    - Map `sfDatabase`, `sfSchema`, `sfWarehouse` options to `USE DATABASE/SCHEMA/WAREHOUSE` statements
    - The `from snowflake.snowpark_connect.snowflake_session import SnowflakeSession` import should appear once per file

9. **Wildcard/Glob Patterns in File Reads**: Wildcard patterns (e.g., `*.json`, `*.csv`, `*.parquet`, `**/*.json`) in `spark.read.json()`, `spark.read.csv()`, `spark.read.parquet()`, or `.load()` are **not supported** in SCOS. They will fail at runtime with `SparkConnectGrpcException: AssertionError (ERROR CODE: 5001)`.

    **Detection**: Look for file read calls where the path argument contains `*`, `?`, `[`, or other glob characters:
    ```python
    # These patterns WILL FAIL in SCOS:
    df = spark.read.json("@MY_STAGE/*.json")
    df = spark.read.csv("s3://bucket/data/*.csv")
    df = spark.read.parquet("/path/to/**/*.parquet")
    df = spark.read.json("data/prefix_*.json")
    ```

    **Fix**: Replace wildcard reads with explicit file lists. Enumerate the individual files that the glob would match and pass them as a list:
    ```python
    # BEFORE (not supported in SCOS):
    df = spark.read.json("@MY_STAGE/*.json")

    # AFTER (supported):
    df = spark.read.json([
        "@MY_STAGE/file1.json",
        "@MY_STAGE/file2.json",
        "@MY_STAGE/file3.json"
    ])
    ```

    If the exact file list is not known at migration time (e.g., the wildcard was reading dynamically generated files), add a TODO with the fix pattern:
    ```python
    # SCOS: TODO - [SPRKCNTPY1000] Wildcard glob "*.json" is not supported in SCOS.
    # Replace with explicit file list: spark.read.json(["@STAGE/f1.json", "@STAGE/f2.json"])
    df = spark.read.json("@MY_STAGE/*.json")
    ```

10. **UDF Serialization (ALL UDF patterns: `udf()`, `@udf`, `@pandas_udf`, `applyInPandas`, `mapInPandas`, factory-style `udf()` calls)**: When the workload uses UDFs that call helper functions, reference module-level variables, or import external modules, these will fail on Snowflake's server-side worker because cloudpickle serializes function references that point to the workload module (which doesn't exist on the server). **Read** `references/python/udf-dependencies.md` (Part 2) for the tiered fix approach:
    - **Tier 1 (Preferred)**: Use `snowpark.connect.udf.packages` for Anaconda packages and `snowpark.connect.udf.python.imports` for custom modules uploaded to a stage. Import inside the UDF body.
    - **Tier 2**: For UDFs with simple logic (including factory-style `udf()` calls that return `udf(fn, type)`), keep all logic self-contained (inline) inside the closure body. Move all imports (`import datetime`, `import ast`, etc.), constants, and helper functions inside the UDF function body so cloudpickle captures them by value. Do NOT replace working UDFs with built-in SQL functions — apply the minimal fix to make the closure self-contained.
    - **Tier 3**: For complex UDFs that call many tightly-coupled helper functions in the same file, use the factory function pattern (to capture data in closures) and `__module__ = "__main__"` patching (to force serialization by value) on the UDF and **all** helper functions in its call chain.

    ```python
    # Example: Tier 3 — factory + __module__ patching
    def make_process_udf(config_dict):
        """Factory captures config in closure."""
        def process_udf(pdf):
            result = helper_a(pdf, config_dict)
            return helper_b(result)
        return process_udf

    process_udf = make_process_udf(my_config)
    for _fn in [process_udf, helper_a, helper_b]:
        _fn.__module__ = "__main__"

    result = df.groupby("key").applyInPandas(process_udf, schema=output_schema)
    ```

11. **Server-Side Package Availability**: When UDFs import third-party packages, verify they are available in Snowflake's Anaconda channel or use PyPI via artifact repository. **Read** `references/python/udf-dependencies.md` (Part 1) for details. If a package is missing from Anaconda:
    - Use PyPI via artifact repository (recommended): `spark.conf.set("snowpark.connect.artifact_repository", "snowflake.snowpark.pypi_shared_repository")`
    - Or replace with a stdlib/numpy-only implementation.
    - Or upload a pure-Python package via `snowpark.connect.udf.python.imports`.

    To check Anaconda availability:
    ```sql
    SELECT * FROM INFORMATION_SCHEMA.PACKAGES
    WHERE LANGUAGE = 'python' AND PACKAGE_NAME ILIKE '%<package>%';
    ```

    To use PyPI:
    ```python
    spark.conf.set("snowpark.connect.artifact_repository", "snowflake.snowpark.pypi_shared_repository")
    spark.conf.set("snowpark.connect.udf.packages", "[package1, package2]")
    ```

12. **`checkpoint()` Not Supported**: `DataFrame.checkpoint()` is not supported in SCOS and will fail at runtime. Replace it with `cache()`, which provides equivalent in-memory persistence behavior.

    ```python
    # BEFORE (not supported in SCOS):
    df = spark.createDataFrame(data, schema)
    df.checkpoint(False)

    # AFTER (supported):
    df = spark.createDataFrame(data, schema)
    # SCOS: [SPRKCNTPY1000] checkpoint() not supported — replaced with cache()
    df.cache()
    ```

    This also applies to `localCheckpoint()` and any variant of `checkpoint(eager)`. In all cases, replace with `cache()`.

13. **Map Column Subscript with Column Key**: Using bracket indexing on a map column with another column as the key (e.g., `map_col[col("key")]`) is **not supported** in Spark Connect. The `Column.__getitem__` method only accepts literal values, not other `Column` objects. It fails at runtime with `PySparkTypeError: [UNSUPPORTED_DATA_TYPE] Unsupported DataType 'Column'`.

    **Detection**: Look for bracket indexing on a map-typed column where the index is a `col()` or `Column` expression:
    ```python
    # These patterns WILL FAIL in SCOS:
    result = df.withColumn("val", category_map[col("category_code")])
    result = df.select(my_map_col[col("lookup_key")])
    result = df.withColumn("v", create_map(lit("a"), lit(1), lit("b"), lit(2))[col("key")])
    ```

    **Fix**: Replace bracket indexing with `element_at()` from `pyspark.sql.functions`, which accepts `Column` arguments and works in both classic and Connect modes:
    ```python
    from pyspark.sql.functions import element_at

    # BEFORE (not supported in SCOS):
    result = df.withColumn("val", category_map[col("category_code")])

    # AFTER (supported):
    # SCOS: [SPRKCNTPY1000] Map column subscript with Column key replaced
    # with element_at() for Spark Connect compatibility
    result = df.withColumn("val", element_at(category_map, col("category_code")))
    ```

    **Note**: Bracket indexing with **literal** keys (e.g., `map_col["some_string"]`) still works. Only `Column`-typed keys trigger this error.

14. **SparkContext Property Access**: Direct access to `SparkContext` properties (e.g., `spark.sparkContext.appName`, `spark.sparkContext.master`, `spark.sparkContext.getConf()`) is not supported in Snowpark Connect. These properties either have static fallback values or should be replaced with configuration lookups.

    ```python
    # BEFORE (not supported in SCOS):
    app_name = spark.sparkContext.appName
    master = spark.sparkContext.master

    # AFTER (supported):
    # SCOS: [SPRKCNTPY1000] SparkContext property replaced with static fallback
    app_name = spark.conf.get("spark.app.name", "scos-app")
    master = "snowflake"  # SCOS runs on Snowflake, no master URL
    ```

15. **Hadoop Filesystem Access**: Hadoop filesystem patterns (`spark.sparkContext._jvm.org.apache.hadoop`, `FileSystem.get()`, `hdfs://` paths) are not available in Snowpark Connect. Replace with Snowflake stage operations or cloud-native SDK calls.

    ```python
    # BEFORE (not supported in SCOS):
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())

    # AFTER:
    # SCOS: TODO - [SPRKCNTPY1000] Hadoop filesystem access not available in SCOS.
    # Replace with Snowflake stage operations (session.file.put/get) or cloud SDK (boto3/azure-storage).
    ```

16. **USE DATABASE/SCHEMA Statements**: Explicit `USE DATABASE` or `USE SCHEMA` statements should be replaced with fully-qualified table references to avoid session state dependencies.

    ```python
    # BEFORE:
    spark.sql("USE DATABASE my_db")
    df = spark.sql("SELECT * FROM my_table")

    # AFTER:
    # SCOS: USE DATABASE replaced with fully-qualified reference
    df = spark.sql("SELECT * FROM my_db.public.my_table")
    ```

17. **JVM-Only Library Imports (Deequ, pydeequ)**: Libraries that depend on the JVM (Deequ, pydeequ, Hive LLAP connectors) are not available in Snowpark Connect. Replace with native DataFrame validation or Snowflake data quality features.

    ```python
    # BEFORE (not supported in SCOS):
    from pydeequ.checks import Check
    check = Check(spark, "data quality")

    # AFTER:
    # SCOS: TODO - [SPRKCNTPY2500] pydeequ/Deequ requires JVM. Replace with native
    # DataFrame checks: df.filter(col("x").isNull()).count() == 0
    ```

18. **ML Pipeline Patterns**: PySpark ML pipeline components (`VectorAssembler`, `Pipeline`, `CrossValidator`, etc.) are not supported in Snowpark Connect. Flag for manual migration to Snowpark ML or scikit-learn.

    ```python
    # SCOS: TODO - [SPRKCNTPY2500] ML pipeline requires manual migration.
    # Consider Snowpark ML (snowflake.ml) or scikit-learn as alternatives.
    ```

19. **UDTF/UDAF Patterns**: PySpark `@udtf` decorators and UDAF patterns need structural transformation to Snowpark handler classes with `process()`/`endPartition()` (UDTF) or `accumulate()`/`merge()`/`finish()` (UDAF) methods.

    ```python
    # BEFORE (not supported as-is in SCOS):
    @udtf(returnType="id: int, doubled: int")
    class DoubleUDTF:
        def eval(self, id, val):
            yield id, val * 2

    # AFTER:
    # SCOS: [SPRKCNTPY2000] UDTF converted to Snowpark handler class
    class DoubleUDTF:
        def process(self, id, val):
            yield (id, val * 2)
        def endPartition(self):
            pass
    # Register with: session.udtf.register(DoubleUDTF, ...)
    ```

20. **Delta Lake Patterns**: Delta Lake operations (`DeltaTable.forPath()`, `MERGE INTO`, `.format("delta")`) are not available in Snowpark Connect. Replace with Snowflake table operations.

    ```python
    # BEFORE (not supported in SCOS):
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, "/path/to/delta")

    # AFTER:
    # SCOS: TODO - [SPRKCNTPY1000] Delta Lake not available. Use Snowflake tables:
    # df = spark.sql("SELECT * FROM my_table")
    ```

21. **Lazy View Re-Evaluation**: When a `createOrReplaceTempView()` is defined once but referenced multiple times in downstream operations, Snowpark Connect may re-evaluate the underlying query each time. Insert `.cache()` after view creation to materialize and prevent redundant computation.

    ```python
    # BEFORE (potential performance issue):
    df.createOrReplaceTempView("my_view")
    result1 = spark.sql("SELECT * FROM my_view WHERE x > 1")
    result2 = spark.sql("SELECT COUNT(*) FROM my_view")

    # AFTER:
    # SCOS: Performance tip - cached view to prevent re-evaluation
    df.cache()
    df.createOrReplaceTempView("my_view")
    result1 = spark.sql("SELECT * FROM my_view WHERE x > 1")
    result2 = spark.sql("SELECT COUNT(*) FROM my_view")
    ```

22. **Memory Anti-Patterns & Known Issues**: Patterns like `.toPandas()` on large DataFrames, `.collect()` in loops, or broadcasting large objects can cause memory issues in Snowpark Connect. The analyzer flags these with specific `how_to_fix` guidance — follow the provided fix. Common patterns:
    - `.toPandas()` on large DataFrames → add `.limit(N)` or use `to_pandas_batches()`
    - `.collect()` in tight loops → refactor to use DataFrame operations
    - `broadcast()` with large tables → let Snowflake's optimizer handle join strategies

---

## Issue Processing Checklist

After processing all issues from `analysis.json`, verify completeness:

- [ ] Every issue in `analysis.json` has been reviewed
- [ ] All high-risk issues (`final_risk` >= 0.7) have fixes applied
- [ ] All medium-risk issues (`final_risk` >= 0.3) have fixes or TODO comments
- [ ] All low-risk issues (`final_risk` < 0.3) have fixes or TODO comments

### Files with No Issues

For files in the manifest that had **no issues** reported by the analysis tool: no changes are needed in this step. These files will still be processed for import updates and migration headers. Confirm you have accounted for them:

```
Step 3 Summary:
  Files with fixes applied: N
  Files with no issues:     M
  Total in manifest:        N + M  ← must match manifest count
```

**Do NOT proceed to import updates until ALL issues have been addressed and the file count is confirmed.**
