# SCOS Migration & Validation Troubleshooting

Common issues and solutions for migrating and validating PySpark workloads with Snowpark Connect (SCOS).

---

## Setup & Environment Issues

### Error: uv not found

**Cause:** The `uv` package manager is not installed.

**Fix:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
Restart your terminal after installation.

---

### Error: Snowflake connection failed

**Cause:** The Snowflake connection is not configured or credentials are invalid.

**Fix:**
- Verify the `default` connection is configured (or use `--connection <name>`)
- Check credentials and network connectivity
- Ensure your Snowflake account is accessible

---

### Error: RAG resources exist but access denied

**Cause:** The RAG knowledge base was set up by another user and you don't have access.

**Fix:** Ask your Snowflake admin to grant access. Replace `<database>` with the database
shown by `SHOW CORTEX SEARCH SERVICES` (e.g. `SCOS_MIGRATION_DVP`):
```sql
GRANT USAGE ON DATABASE <database> TO ROLE <your_role>;
GRANT USAGE ON SCHEMA <database>.PUBLIC TO ROLE <your_role>;
GRANT SELECT ON TABLE <database>.PUBLIC.SCOS_COMPAT_ISSUES TO ROLE <your_role>;
GRANT USAGE ON CORTEX SEARCH SERVICE <database>.PUBLIC.SCOS_COMPAT_ISSUES_SERVICE TO ROLE <your_role>;
```

---

## Migration Issues

### Error: Analysis returns empty results

**Cause:** The path doesn't contain PySpark code or `.py` files.

**Fix:**
- Verify the path contains `.py` files
- Check if files contain PySpark code (imports from `pyspark`)

---

### Error: Syntax check fails after migration

**Cause:** Incomplete edits or malformed code introduced during migration.

**Fix:**
- Review the specific file for incomplete edits
- Check for mismatched quotes or brackets in string replacements
- Run `python3 -m py_compile <file>` to identify the exact syntax error

---

### Error: Import errors after migration

**Cause:** Unsupported imports remain or `snowpark_connect` initialization is incorrect.

**Fix:**
- Ensure unsupported imports (`databricks`, `delta`) are removed
- Verify `snowpark_connect` initialization is correct:
  ```python
  from snowflake import snowpark_connect
  spark = snowpark_connect.init_spark_session()
  ```

---

## Validation Issues

### ImportError

**Cause:** The workload module cannot be found.

**Fix:** Ensure `sys.path` includes the parent directory of the `_scos` workload:
```python
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
```

---

### Module-level code fails

**Cause:** External data is accessed at module import time, before tables are created.

**Fix:** Create ALL Snowflake tables BEFORE importing the workload module. In your entrypoint:
```python
# 1. Initialize Spark session
spark = snowpark_connect.init_spark_session()

# 2. Create ALL synthetic tables FIRST
df.write.mode("overwrite").saveAsTable("my_table")

# 3. THEN import the workload (after tables exist)
from my_workload import main_function
```

---

### Schema mismatch at runtime

**Cause:** Synthetic data schema doesn't match what the workload expects.

**Fix:** Re-check column names and types used downstream in the workload and update synthetic data to match. Trace column usage through selects, joins, filters, and casts.

---

### Missing dependency

**Cause:** The workload imports a module that isn't available in the test environment.

**Fix:** Install the missing dependency or report it as a limitation. Check if the package is available:
```bash
pip install <package_name>
```

---

### Stage creation fails

**Cause:** Warehouse is inactive or user lacks permissions.

**Fix:**
- Verify the warehouse is active: `ALTER WAREHOUSE <name> RESUME`
- Verify the user has `CREATE STAGE` privilege
- Check that the database and schema in the active session are accessible

---

### PUT/upload fails

**Cause:** File path issues or stage problems.

**Fix:**
- Verify the local file path exists
- Verify the stage was created successfully
- Ensure the warehouse is running
- Use `auto_compress=False` to avoid double-compression issues:
  ```python
  session.file.put('file:///path/to/file', '@STAGE_NAME/', auto_compress=False)
  ```

---

### spark.read from stage fails

**Cause:** Incorrect stage path format or file format mismatch.

**Fix:**
- Verify the stage path uses the `@` prefix (e.g., `@STAGE_NAME/path/file.parquet`)
- Confirm the uploaded file format matches the read method:
  - `.read.parquet()` expects a Parquet file
  - `.read.csv()` expects a CSV file
  - Don't mix formats

---

### spark.read with wildcard pattern fails (AssertionError, ERROR CODE: 5001)

**Cause:** Wildcard/glob patterns (`*.json`, `*.csv`, `*.parquet`, `**/*.parquet`) in file read paths are not supported in SCOS. The SCOS runtime cannot resolve glob patterns and throws `SparkConnectGrpcException: AssertionError`.

**Example error:**
```
SparkConnectGrpcException: AssertionError (ERROR CODE: 5001)
```

Triggered by code like:
```python
df = spark.read.json("@MY_STAGE/*.json")
df = spark.read.csv("s3://bucket/data/*.csv")
```

**Fix:** Replace wildcard reads with explicit file lists:
```python
# BEFORE (fails):
df = spark.read.json("@MY_STAGE/*.json")

# AFTER (works):
df = spark.read.json([
    "@MY_STAGE/file1.json",
    "@MY_STAGE/file2.json",
    "@MY_STAGE/file3.json"
])
```

If the file list is dynamic, use Snowflake's `LIST @STAGE` to enumerate files first:
```python
from snowflake.snowpark_connect.snowflake_session import SnowflakeSession
snowflake_session = SnowflakeSession(spark)
files_df = snowflake_session.sql("LIST @MY_STAGE PATTERN='.*\\.json'")
file_list = ["@MY_STAGE/" + row["name"].split("/")[-1] for row in files_df.collect()]
df = spark.read.json(file_list)
```

---

### Schema mismatch after stage read

**Cause:** Inferred schema doesn't match the synthetic data.

**Fix:**
- Re-check inferred columns against actual read options
- For CSV files with `header=True`, ensure the generated CSV has a header row
- For `inferSchema=True`, ensure synthetic data types are consistent within each column

---

### Error: PySparkTypeError UNSUPPORTED_DATA_TYPE on map column subscript

**Cause:** Using `map_col[col("key")]` to index a map column with another `Column` as the key. In Spark Connect, `Column.__getitem__` only accepts literal values, not `Column` expressions.

**Example error:**
```
PySparkTypeError: [UNSUPPORTED_DATA_TYPE] Unsupported DataType 'Column'
```

Triggered by code like:
```python
category_map = create_map(lit("A"), lit(1), lit("B"), lit(2))
result = df.withColumn("val", category_map[col("category_code")])
```

**Fix:** Replace bracket indexing with `element_at()`, which accepts `Column` arguments and works in both classic and Connect modes:
```python
from pyspark.sql.functions import element_at

# BEFORE (fails in Connect):
result = df.withColumn("val", category_map[col("category_code")])

# AFTER (works in both):
result = df.withColumn("val", element_at(category_map, col("category_code")))
```

Bracket indexing with **literal** keys (e.g., `map_col["some_string"]`) still works.

---

### Error: PySpark 4.x protobuf conflict with snowpark-connect

**Cause:** PySpark 4.x generates protobuf code requiring runtime >= 6.33.0, but `snowpark-connect-deps` pins protobuf at 6.31.1. Additionally, `snowpark-connect-deps` bundles its own `spark/connect/types.proto` descriptor which conflicts with PySpark 4.x's copy.

**Example errors:**
```
google.protobuf.runtime_version.VersionError
```
or
```
duplicate file name spark/connect/types.proto
```

**Fix:** The skill's `pyproject.toml` pins `pyspark>=3.5,<4`. Always run commands via `uv run --project <SKILL_DIRECTORY>` to use the pinned version. If PySpark 4.x is installed globally, `uv run --project` isolates the environment.

---

### Error: No local Java runtime for PySpark

**Cause:** PySpark local-mode execution requires a JVM. The machine may have no system JDK, or PySpark 4.x may require JDK 19+ while only JDK 17 is available.

**Fix:** The skill's `pyproject.toml` uses `snowpark-connect[jdk]` which installs `jdk4py` (JDK 17). Always run commands via `uv run --project <SKILL_DIRECTORY>` — this makes the bundled JDK available automatically. Do not rely on a system JDK.

**Note:** `jdk4py` provides JDK 17, compatible with PySpark 3.5.x but not PySpark 4.x (which requires JDK 19+). This is another reason the `pyspark>=3.5,<4` pin is necessary.

---

## UDF & Serialization Issues

### ModuleNotFoundError: No module named '<workload_module_name>'

**Cause:** `applyInPandas`/`mapInPandas`/UDF functions call helper functions defined in the workload file. cloudpickle serializes them by module reference, and the server-side worker can't import the workload module.

**Fix:** Apply the tiered approach from `references/udf-dependencies.md` (Part 2):

- **Tier 1 (Preferred):** Upload helpers to stage + `snowpark.connect.udf.python.imports`
- **Tier 3:** `__module__ = "__main__"` patching on the UDF and all helper functions in its call chain, plus factory functions for captured globals

Example Tier 3 fix:
```python
def make_process_udf(config):
    def process_udf(pdf):
        return helper_fn(pdf, config)
    return process_udf

process_udf = make_process_udf(my_config)
for fn in [process_udf, helper_fn]:
    fn.__module__ = "__main__"
```

Apply fixes to the **original** `_scos` file (not just the test copy), then re-copy to the test directory.

---

### ModuleNotFoundError: No module named '<package_name>'

**Cause:** A UDF imports a third-party package that isn't available on Snowflake's server-side Python worker.

**Fix:** You have three options:

1. **Use PyPI via artifact repository (Recommended):**
   ```python
   spark.conf.set("snowpark.connect.artifact_repository", "snowflake.snowpark.pypi_shared_repository")
   spark.conf.set("snowpark.connect.udf.packages", "[package_name]")
   ```

2. **Check if available in Anaconda channel:**
   ```sql
   SELECT * FROM INFORMATION_SCHEMA.PACKAGES 
   WHERE LANGUAGE = 'python' AND PACKAGE_NAME ILIKE '%<package>%';
   ```
   If found, declare it:
   ```python
   spark.conf.set("snowpark.connect.udf.packages", "[package_name]")
   ```

3. **Replace with stdlib/numpy-only implementation** if the package isn't available anywhere.

See `references/udf-dependencies.md` (Part 1) for detailed guidance.

Apply fixes to the **original** `_scos` file, then re-copy to the test directory.
