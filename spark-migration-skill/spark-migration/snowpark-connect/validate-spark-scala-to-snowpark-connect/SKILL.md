---
name: validate-spark-scala-to-snowpark-connect
description: |
  Validate a completed Spark Scala to Snowpark Connect (SCOS) migration by running the migrated
  workload end-to-end with synthetic data. Use when: verifying Scala migration correctness,
  smoke-testing migrated Scala code, checking SCOS runtime compatibility for Scala.
  Triggers: validate scala migration, verify scala scos, test scala migration, smoke test scala.
parent_skill: snowpark-connect
allowed-tools: Read, Write, Bash
---

# Validate Spark Scala to Snowpark Connect Migration

Run the migrated `_scos` Scala workload by compiling it and executing the main entry point with synthetic data.

## When to Load

[snowpark-connect] Intent Detection: After user indicates validation intent for Scala code (validate, verify, check, test, review migration).

## Arguments

- `$ARGUMENTS` - Path to migrated `_scos` Scala script, project, or directory.

**Derived names used throughout:**
- `<workload>` = basename of `$ARGUMENTS` (e.g., if `$ARGUMENTS` is `/path/to/my_pipeline_scos/`, then `<workload>` = `my_pipeline_scos`)
- `<workload>_test/` = test directory created alongside the workload

## Rules

1. **Never modify the migrated workload.** Create an editable copy in `<workload>_test/` and add an entrypoint that triggers the main execution flow.
2. **Create synthetic data for all external sources.** Mock Category A sources (`spark.table()`) by checking if tables exist and creating them if permitted. Mock Category B sources (`spark.read.*` with cloud/local paths) by uploading synthetic files to a Snowflake stage and replacing paths in the workload copy.
3. **Minimal synthetic data.** 2-5 rows per source. Only include columns actually used by the workload.
4. **CRITICAL: Call the REAL workload functions.** The entrypoint must invoke the actual migrated code. Do NOT rewrite or duplicate workload logic.
5. **This is a smoke test, not a unit test suite.** The goal is to verify the workload runs end-to-end without exceptions, not to test individual operations or assert data correctness.

## Prerequisites

### Skill Directory

`<SKILL_DIRECTORY>` in this document refers to the **parent** skill directory (`snowpark-connect/`) — the directory containing `pyproject.toml` and the `scripts/` folder. It is one level above this sub-skill's directory. To resolve it, find the nearest ancestor directory that contains `pyproject.toml`:

```bash
# From this sub-skill's directory, go up one level:
SKILL_DIRECTORY="$(cd "$(dirname "$0")/.." && pwd)"
# Or locate by marker file:
SKILL_DIRECTORY="$(git rev-parse --show-toplevel)/data-engineering/spark-migration/snowpark-connect"
```

### Environment Checks

```bash
uv --version || echo "PREREQ_FAIL: uv not installed"
# Verify Snowflake connection works
uv run --project <SKILL_DIRECTORY> \
  python -c "from snowflake.snowpark import Session; s = Session.builder.config('connection_name', 'default').create(); print('OK')" \
  || echo "PREREQ_FAIL: Snowflake connection failed"

# Verify Scala build tools
sbt --version || scalac -version || echo "PREREQ_FAIL: No Scala build tool found"
```

---

## Workflow

You **MUST** perform the phases below **in order**.

### Phase 1: Analyze Workload

#### 1.1 Validate migrated workload exists

```bash
test -e "$ARGUMENTS" || echo "ABORT: Migrated workload not found"
```

#### 1.2 Identify and classify external data dependencies

Find all external data access in the workload: `spark.read.*`, `spark.table()`, `spark.sql("SELECT ... FROM ...")`.

**For directory workloads**, scan **ALL `.scala` files** in the directory. Data reads may occur in any module.

Classify each data source into one of two categories:

**Category A — Table-mockable** (handled via Snowflake tables):
- `spark.table("table_name")` calls
- `spark.sql("SELECT ... FROM table_name")` calls

For each, determine:
- Fully qualified table name
- Column names and types (infer from downstream usage)

**Category B — Stage-mockable** (requires Snowflake stage with synthetic files):
- `spark.read.csv("s3://...")`, `spark.read.parquet("gs://...")`, or any `spark.read.*` call with an external cloud path or local path
- Also check for `spark.read.format(...).load("path")` variants

For each file read, capture:
- **Read method**: `csv`, `parquet`, `json`, `text`, or `format(...).load(...)`
- **Full path string**
- **Read options**: e.g., `header=true`, `inferSchema=true`
- **Downstream column usage**: trace how the DataFrame is used

#### 1.2.1 Stopping Point — External File Reads

If **any Category B (stage-mockable) reads** were found, you **MUST** pause and alert the user:

```
⚠️  External cloud reads detected in the migrated workload:
  1. spark.read.<method>("<path>")
  2. ...

These paths reference external cloud storage.
For validation, I need a Snowflake stage to upload synthetic test data.

Do you already have an external stage for these locations?
  - If YES: provide the stage name and I'll upload mock data files to it.
  - If NO: I'll create an internal stage (SCOS_VALIDATION_<workload>)
    and upload synthetic files there for testing.
```

Wait for the user to respond before proceeding.

#### 1.2.2 Stopping Point — Table Dependencies

If **any Category A (table-mockable) reads** were found, check table existence and prompt user.

---

#### 1.3 Stage Setup for External Reads

> Skip this phase entirely if no Category B reads were detected.

Follow the same stage creation and mock data upload process as the PySpark validation skill. Generate mock data files matching the expected format and upload to the Snowflake stage.

---

#### 1.4 Analyze workload hierarchy and find entrypoint

Read **ALL files** in the `_scos` workload. Build a complete picture:
- Which objects/classes exist
- Which imports exist between files
- What is the call graph from `main()` to lower-level functions

The **main entrypoint** is the `object` with `def main(args: Array[String])` that orchestrates the pipeline.

**CRITICAL**: The entrypoint must be identified by understanding the full workload structure, not by guessing.

#### 1.5 CHECKPOINT: Display Analysis Summary

**Display the analysis summary, then proceed automatically to Phase 2.**

```
════════════════════════════════════════════════════════════
WORKLOAD ANALYSIS COMPLETE
════════════════════════════════════════════════════════════
Workload: $ARGUMENTS
Type: [.scala file | sbt project | directory]
Identified Entrypoint:
  Object: <ObjectName>.main() in <file>
  Call hierarchy: main() → process() → transform()
Data Dependencies (N total):
  Tables (spark.table / Category A):
    - catalog.schema.table1
  File Reads (spark.read.* / Category B):
    - s3://bucket/path.json (JSON)
════════════════════════════════════════════════════════════
```

---

### Phase 2: Setup Test Directory

#### 2.1 Copy workload and create entrypoint

```bash
mkdir -p <workload>_test/
cp -r $ARGUMENTS <workload>_test/
```

**If Category B reads exist:** Replace external paths with stage paths in the copied workload files.

Create a test entrypoint. For Scala workloads, this can be either:

**Option A: Scala test object** — if the workload is an sbt project, create a test Scala file:
```scala
/*
 * Full workload test entrypoint for <workload_name> SCOS migration.
 * Initializes synthetic data and runs the processing pipeline.
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object SCOSValidationEntrypoint {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .getOrCreate()

    // Create synthetic tables BEFORE calling workload
    val schema = StructType(Seq(
      StructField("id", StringType, true),
      StructField("amount", DecimalType(38, 18), true)
    ))
    val data = java.util.Arrays.asList(
      Row("1", new java.math.BigDecimal("100.00")),
      Row("2", new java.math.BigDecimal("200.00"))
    )
    spark.createDataFrame(data, schema)
      .write.mode("overwrite").saveAsTable("<table_name>")

    // Call the REAL workload
    <WorkloadObject>.main(Array())

    println("SUCCESS: Workload completed")
    spark.stop()
  }
}
```

**Option B: Python entrypoint** — launch the Snowpark Connect server from Python, then run the Scala workload via sbt:
```python
"""
Validation entrypoint — starts SCOS server, creates synthetic data,
then runs the Scala workload via sbt.
"""
import subprocess
from snowflake import snowpark_connect

spark = snowpark_connect.init_spark_session()

# Create synthetic tables
# ... (same pattern as PySpark validation)

# Run the Scala workload
result = subprocess.run(
    ["sbt", "runMain", "<WorkloadObject>"],
    cwd="<workload_test_dir>",
    capture_output=True, text=True
)
print(result.stdout)
if result.returncode != 0:
    print(result.stderr)
    raise SystemExit(result.returncode)
print("SUCCESS: Workload completed")
```

Choose the approach that best fits the workload structure.

#### 2.2 Synthetic data generation rules

- **Minimal data:** 2-5 rows per table
- **Cover key paths:** Include at least one row that matches join conditions
- **Use realistic types:** Match the expected schema

#### 2.3 Schema inference

Infer schema from how data is used in the workload:
- Column references: `df.select("col1", "col2")` → columns col1, col2
- Type hints: `.cast("int")`, `IntegerType` → integer column
- Operations: `df.filter($"amount" > 0)` → numeric column
- Joins: `df1.join(df2, "key")` → both have "key" column
- Explicit schemas: If workload defines `StructType(...)`, use those exact columns/types

---

### Phase 3: Run Entrypoint

```bash
cd <workload>_test/

# Option A: sbt-based
sbt "runMain SCOSValidationEntrypoint" > output.log 2>&1
EXIT_CODE=$?

# Option B: Python-based
uv run --project <SKILL_DIRECTORY> python entrypoint.py > output.log 2>&1
EXIT_CODE=$?
```

**CRITICAL — On ANY failure** (`EXIT_CODE != 0`):
1. Read `output.log` and include the full error details in the Phase 4 report.
2. **STOP and present the report to the user.** Wait for user reaction before taking any further action.
3. Do NOT attempt to fix the error. Do NOT retry the command.

---

### Phase 4: Report

```
════════════════════════════════════════════════════════════
FULL WORKLOAD TEST
════════════════════════════════════════════════════════════

Workload: $ARGUMENTS
Test directory: <workload>_test/
Entrypoint: SCOSValidationEntrypoint / entrypoint.py

Workload functions called:
- <ObjectName>.main() from <file>

Mocked tables:
- <fully_qualified_table_name> (N rows): col1, col2, ...

Stage-Mocked External Reads:                        ← omit if none
  Stage: <STAGE_NAME> (preserved for inspection)
  Files:
  - @<STAGE_NAME>/<subpath>/<filename> (N rows): col1, col2, ...
    Original: <original_cloud_path>

Exit code: <0 or N>

Output:
<show relevant output or errors>

════════════════════════════════════════════════════════════
RESULT: ✅ SUCCESS | ❌ FAILED
════════════════════════════════════════════════════════════
```

---

### Phase 5: Cleanup Snowflake Resources

After presenting the Phase 4 report, list the Snowflake resources created during validation and prompt the user. **Do NOT offer to delete the test directory.**

```
Snowflake resources created during validation:
  - Tables: <table1>, <table2>... (if created by agent)
  - Stage: <STAGE_NAME> (if created by agent)

Would you like to clean up these Snowflake resources?
```

If the user accepts, delete in order:
1. Drop created tables
2. Drop the Snowflake stage (only if agent created it)

---

## Stopping Points

- Phase 1.2.1: If external file reads detected — prompt user for stage name
- Phase 1.2.2: If table dependencies detected — check existence and prompt user
- Phase 1: After analyzing workload hierarchy — verify entrypoint and data dependencies
- Phase 3 (on failure): Report error and stop. Do NOT attempt to fix or retry.
- Phase 4: After reporting results
- Phase 5: After report — list resources and ask user whether to clean up

## Success Criteria

- The workload's main function(s) execute without throwing exceptions
- The migrated code is compilable and compatible with SCOS APIs
- All Category A data sources are mocked as Snowflake tables or existing tables are used
- All Category B data sources are mocked via Snowflake stage with synthetic files
- The entrypoint calls real workload functions (not reimplemented logic)

## Output

Validation report (Phase 4 format) summarizing pass/fail status, workload functions called, mocked tables, and any errors encountered.

## Troubleshooting

See `references/scala/troubleshooting.md` for common issues and solutions.
