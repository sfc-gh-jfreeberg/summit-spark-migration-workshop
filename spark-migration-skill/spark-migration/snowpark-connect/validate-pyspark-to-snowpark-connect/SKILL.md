---
name: validate-pyspark-to-snowpark-connect
description: |
  Validate a completed PySpark to Snowpark Connect (SCOS) migration by running the migrated
  workload end-to-end with synthetic data. Use when: verifying migration correctness,
  smoke-testing migrated code, checking SCOS runtime compatibility.
  Triggers: validate migration, verify scos, test migration, smoke test, run migrated workload.
parent_skill: snowpark-connect
allowed-tools: Read, Write, Bash
---

# Validate PySpark to Snowpark Connect Migration

Run the migrated `_scos` workload by importing its real functions/classes and executing them with synthetic data.

## When to Load

[snowpark-connect] Intent Detection: After user indicates validation intent (validate, verify, check, test, review migration).

## Arguments

- `$ARGUMENTS` - Path to migrated `_scos` script, notebook, or directory.

**Derived names used throughout:**
- `<workload>` = basename of `$ARGUMENTS` (e.g., if `$ARGUMENTS` is `/path/to/my_pipeline_scos.py`, then `<workload>` = `my_pipeline_scos`)
- `<workload>_test/` = test directory created alongside the workload

## Rules

1. **Never modify the migrated workload.** Create an editable copy in `<workload>_test/` and add a single `entrypoint.py` file that triggers the main execution flow.
2. **Create synthetic data for all external sources.** Mock Category A sources (`spark.table()`) by checking if tables exist and creating them if permitted. Mock Category B sources (`spark.read.*` with cloud/local paths) by uploading synthetic files to a Snowflake stage and replacing paths in the workload copy (see Phases 1.3 and 2.1).
3. **Minimal synthetic data.** 2-5 rows per source. Only include columns actually used by the workload.
4. **CRITICAL: Import and call the REAL workload functions.** The entrypoint must import the actual migrated code (e.g., `from modeling_library import model, load_data`) and call it. Do NOT rewrite or duplicate workload logic — no independent test cases like "Test window functions" or "Test joins".
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

### PySpark Version & JDK

All commands in this skill **must** use `uv run --project <SKILL_DIRECTORY>` which provides:
- **PySpark 3.5.x** — pinned in `pyproject.toml` as `pyspark>=3.5,<4`. PySpark 4.x is **not compatible** due to protobuf descriptor conflicts with `snowpark-connect-deps`.
- **JDK 17** via `jdk4py` — bundled by the `snowpark-connect[jdk]` extra. No system JDK is required.

**Do NOT run scripts directly.** Always use `uv run --project <SKILL_DIRECTORY>` to ensure the correct PySpark version and JDK are on the path.

### Environment Checks

```bash
uv --version || echo "PREREQ_FAIL: uv not installed"
uv run --project <SKILL_DIRECTORY> \
  python -c "from snowflake import snowpark_connect; spark = snowpark_connect.init_spark_session(); print('OK')" \
  || echo "PREREQ_FAIL: Snowflake connection failed"

# Check 3 (notebook workloads only): jupyter nbconvert
uv run --project <SKILL_DIRECTORY> \
  jupyter nbconvert --version \
  || echo "PREREQ_FAIL: jupyter nbconvert not installed"
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

Find all external data access in the workload: `spark.read.*`, `spark.table()`, `spark.sql("SELECT ... FROM ...")`, `boto3`/S3.

**For directory workloads**, scan **ALL `.py` files** in the directory — not just the main entrypoint. Data reads may occur in any module (e.g., a `loader.py` or `data_access.py`).

**For single `.py` files**, search the source directly. **For `.ipynb` notebooks**, search within the `source` arrays of code cells in the notebook JSON.

Classify each data source into one of two categories:

**Category A — Table-mockable** (handled via Snowflake tables):
- `spark.table("table_name")` calls
- `spark.sql("SELECT ... FROM table_name")` calls

For each, determine:
- Fully qualified table name (e.g., `database.schema.table_name`)
- Column names and types (infer from downstream usage)

**Category B — Stage-mockable** (requires Snowflake stage with synthetic files):
- `spark.read.csv("s3://...")`, `spark.read.parquet("gs://...")`, `spark.read.json("/mnt/...")`, or any `spark.read.*` call with an external cloud path (`s3://`, `s3a://`, `gs://`, `abfs://`, `wasb://`, `adl://`) or local/mounted path
- Also check for `spark.read.format(...).load("path")` variants

For each file read, capture:
- **Read method**: `csv`, `parquet`, `json`, `text`, or `format(...).load(...)`
- **Full path string**: e.g., `s3://analytics-lake/raw/events/2024/`
- **Read options**: e.g., `header=True`, `inferSchema=True`, `delimiter=","`
- **Downstream column usage**: trace how the resulting DataFrame is used (selects, joins, filters, casts) to infer the schema — same approach as Phase 2.3

#### 1.2.1 Stopping Point — External File Reads

If **any Category B (stage-mockable) reads** were found, you **MUST** pause and alert the user:

```
⚠️  External cloud reads detected in the migrated workload:
  1. spark.read.<method>("<path>")
  2. ...

These paths reference external cloud storage (S3, GCS, Azure Blob, etc.).
Snowflake recommends creating an external stage that points to these
cloud locations for production use.

For validation, I need a Snowflake stage to upload synthetic test data.

Do you already have an external stage for these locations?
  - If YES: provide the stage name and I'll upload mock data files to it.
  - If NO: I'll create an internal stage (SCOS_VALIDATION_<workload>)
    and upload synthetic files there for testing.
```

Wait for the user to respond before proceeding.

- **User provides an existing stage name** → Record it as `<STAGE_NAME>`. Skip stage creation in Phase 1.3.1 (stage already exists). Proceed to Phase 1.3.2 to generate and upload mock data.
- **User says no / no existing stage** → Use the auto-generated name `SCOS_VALIDATION_<workload>` as `<STAGE_NAME>`. Create it as an internal stage in Phase 1.3.1.
- **User provides a custom name for a new stage** → Use that name as `<STAGE_NAME>`. Create it in Phase 1.3.1.

**Use a single stage for ALL mock files.** Do NOT create multiple stages — store all synthetic data files in one stage, organized by subpath (e.g., `@<STAGE_NAME>/events/`, `@<STAGE_NAME>/users/`).

#### 1.2.2 Stopping Point — Table Dependencies

If **any Category A (table-mockable) reads** were found, you **MUST** pause and verify table existence in Snowflake before proceeding:

1. Check if the tables exist using Snowflake `SHOW TABLES` or `SELECT COUNT(*)`.

```bash
uv run --project <SKILL_DIRECTORY> python -c "
from snowflake.snowpark import Session
session = Session.builder.config('connection_name', 'default').create()
try:
    session.sql('SELECT COUNT(*) FROM <fully_qualified_table_name> LIMIT 1').collect()
    print('EXISTS')
except Exception:
    print('MISSING')
"
```

2. Alert the user based on the results:

If tables **DO NOT exist**:
```
⚠️  The workload reads from Snowflake tables that do not exist in your current context:
  1. <fully_qualified_table_name>
  2. ...

I will create synthetic tables with mock data to validate the workload.
Do I have your permission to proceed and create these synthetic tables?
```

If tables **DO exist**:
```
✅ The workload reads from Snowflake tables that already exist in your account:
  1. <fully_qualified_table_name>
  2. ...

Would you like me to use these existing tables for validation, or should I create synthetic tables instead?
```

Wait for the user's permission/choice before proceeding.

- If user chooses existing tables: Skip table creation in Phase 2.
- If user authorizes synthetic tables: Proceed to generate and create synthetic tables in Phase 2.

---

#### 1.3 Stage Setup for External Reads

> Skip this phase entirely if no Category B (stage-mockable) reads were detected in Phase 1.2.

##### 1.3.1 Create the Snowflake stage

> **Skip this step** if the user provided an existing stage name in Phase 1.2.1.

If creating a new internal stage:

```bash
uv run --project <SKILL_DIRECTORY> python -c "
from snowflake.snowpark import Session
session = Session.builder.config('connection_name', 'default').create()
session.sql('CREATE STAGE IF NOT EXISTS <STAGE_NAME>').collect()
print('Stage created: <STAGE_NAME>')
"
```

##### 1.3.2 Generate mock data files

For **each** Category B read detected in Phase 1.2:

1. **Infer schema** from downstream column usage (same approach as Phase 2.3 — look at selects, joins, filters, casts, type hints).
2. **Generate 2-5 rows** of synthetic data covering key paths (e.g., at least one row matching join conditions).
3. **Write a file matching the original format** the workload reads:
   - For `.read.csv(...)` — generate a `.csv` file
   - For `.read.parquet(...)` — generate a `.parquet` file (using `pandas` + `pyarrow`)
   - For `.read.json(...)` — generate a `.json` (one JSON object per line, i.e., JSONL)
   - For `.read.text(...)` — generate a `.txt` file

Write files to a temporary local directory:

```python
import pandas as pd

# Example: generate mock parquet for s3://analytics-lake/raw/events/2024/
events_data = pd.DataFrame({
    "event_id": ["e1", "e2", "e3"],
    "user_id": ["u1", "u2", "u1"],
    "product_id": ["p1", "p2", "p3"],
    "event_type": ["click", "purchase", "click"],
    "event_timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"],
})
events_data.to_parquet("/tmp/scos_validation/mock_events.parquet", index=False)
```

##### 1.3.3 Upload mock files to the stage

For each generated file, upload to the stage:

```bash
uv run --project <SKILL_DIRECTORY> python -c "
from snowflake.snowpark import Session
session = Session.builder.config('connection_name', 'default').create()
session.file.put('file:///tmp/scos_validation/<filename>', '@<STAGE_NAME>/<subpath>/', auto_compress=False, overwrite=True)
print('Uploaded: @<STAGE_NAME>/<subpath>/<filename>')
"
```

Repeat for every Category B source. Use meaningful subpath names derived from the original path (e.g., `events/`, `users/`, `products/`).

---

#### 1.4 Analyze workload hierarchy and find entrypoint

Read **ALL files** in the `_scos` workload (both `.py` and `.ipynb`). Build a complete picture of the module/class/function hierarchy:
- Which modules import which other modules
- Which functions call which other functions
- What is the call graph from top-level to low-level functions

**For `.py` files:** The **main entrypoint** is determined by analyzing this entire hierarchy - it's the function at the TOP of the call graph that orchestrates the entire pipeline. Look for:
- High-level functions like `model()`, `run()`, `main()`, `process()`
- Functions that orchestrate other functions (call load, transform, save)
- `if __name__ == "__main__"` blocks

**For `.ipynb` notebooks:** Notebooks execute top-to-bottom through all code cells. The entire notebook IS the entrypoint. To make it importable for the test entrypoint, convert it to a Python script using `jupyter nbconvert` (verified in Prerequisites):

```bash
# Convert notebook to .py script for import
jupyter nbconvert --to script <notebook>_scos.ipynb --output <notebook>_scos_converted
```

The converted script can then be imported from the entrypoint.

**CRITICAL**: The entrypoint must be identified by understanding the full workload structure, not by guessing based on function names. Trace the call hierarchy to find the top-level function that a real customer would invoke to run the entire pipeline.

#### 1.5 📋 CHECKPOINT: Display Analysis Summary

**Display the analysis summary, then proceed automatically to Phase 2. Do NOT wait for user input.**

```
════════════════════════════════════════════════════════════
WORKLOAD ANALYSIS COMPLETE
════════════════════════════════════════════════════════════
Workload: $ARGUMENTS
Type: [.py file | .ipynb notebook | directory]
Identified Entrypoint:
  Function: <function_name>() in <module>
  Call hierarchy: main() → process() → transform()
Data Dependencies (N total):
  Tables (spark.table / Category A):
    - catalog.schema.table1
    - catalog.schema.table2
  File Reads (spark.read.* / Category B):
    - s3://bucket/path.json (JSON)
    - s3://bucket/events.csv (CSV)
  SQL Queries:
    - SELECT ... FROM table3
════════════════════════════════════════════════════════════
```

Proceed immediately to Phase 2.

---

### Phase 2: Setup Test Directory

#### 2.1 Copy workload and create entrypoint

```bash
mkdir -p <workload>_test/

# Copy the migrated workload (works for both .py files, .ipynb notebooks, and directories)
cp -r $ARGUMENTS <workload>_test/

# For notebook workloads, also convert to .py for importing (see Phase 1.4)

# entrypoint.py will be created in the root of the test directory
```

**If Category B (stage-mockable) reads exist:** In the **copied** workload files inside `<workload>_test/`, replace each external cloud/file path with the corresponding stage path. **For directory workloads, apply replacements across ALL `.py` files** — not just the entrypoint file, since data reads may occur in any module. Preserve all read options (header, inferSchema, delimiter, etc.) — only the path string changes.

For each Category B read, apply the replacement:
```python
# BEFORE (in the copy)
events_df = spark.read.parquet("s3://analytics-lake/raw/events/2024/")

# AFTER (in the copy)
# SCOS Validation: Original path "s3://analytics-lake/raw/events/2024/" replaced with stage path
events_df = spark.read.parquet("@<STAGE_NAME>/events/mock_events.parquet")
```

Add a comment above each replaced line noting the original path for traceability.

Create `<workload>_test/entrypoint.py` in the root of the test directory.

**Strict Entrypoint Requirements:**
1. **Single File:** All test logic resides in `entrypoint.py`.
2. **Order of Operations:** Init Spark -> Create ALL Synthetic Snowflake Tables -> Import Workload -> Call Real Workload Functions.
3. **Data Mocking:** ALL tables accessed via `spark.table()` must be mocked as Snowflake tables BEFORE importing workload modules (since some modules read tables at import time).
4. **Call Real Functions:** Import and call the actual workload functions (e.g., `model()`, `load_data()`). Do NOT rewrite the workload logic in the entrypoint.
5. **Environment:** Must run successfully with `uv run --project <SKILL_DIRECTORY> python entrypoint.py` in the test directory.
6. **NO independent test cases:** Do NOT write code like "Test window functions", "Test joins", etc. The workload code already contains these operations - just call the workload.

**entrypoint.py template:**
```python
"""
Full workload test entrypoint for <workload_name> SCOS migration.
Initializes synthetic data and runs the processing pipeline.
"""
import os
import sys

# Set required environment variables BEFORE any imports
os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"
os.environ["CATALOG"] = "test_catalog"
# ... other env vars the workload expects ...

# Add the workload to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from snowflake import snowpark_connect
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

# Initialize SCOS session
spark = snowpark_connect.init_spark_session()

# ============================================================
# SYNTHETIC DATA - Create Snowflake tables for ALL external tables
# ============================================================
# IMPORTANT: Create these BEFORE importing the workload modules,
# because some modules call spark.table() at import time.

# Example: Mock the main data table
data_schema = StructType([
    StructField("id", StringType(), True),
    StructField("amount", DecimalType(38, 18), True),
    # Add columns used by workload
])
data = [("1", Decimal("100.00")), ("2", Decimal("200.00"))]
df = spark.createDataFrame(data, data_schema)
df.write.mode("overwrite").saveAsTable("<fully_qualified_table_name>")

# ... create ALL other tables the workload accesses ...

# ============================================================
# RUN WORKLOAD - Import and call the REAL functions
# ============================================================
# Now import the workload (after tables are mocked)
from <workload_module> import <main_function>

print("Running workload...")
result = <main_function>(
    # Pass appropriate arguments
)

# Optionally show results
result.show()
print("SUCCESS: Workload completed")
```

**CRITICAL**: The entrypoint MUST call the real workload functions. For example:
- If the workload has `model(data)`, call `model(data)`
- If the workload has `load_data()` followed by `process()`, call both
- Do NOT reimplement the workload logic with simple test operations

**Module-level code handling:**
- If the workload reads tables at module import time, ensure Snowflake tables are created BEFORE importing
- If the workload has `if __name__ == "__main__"` blocks, copy that code to the entrypoint
- For notebook workloads: import the converted `.py` script (from Phase 1.4 conversion) instead of the `.ipynb` file directly. If the notebook has top-level code (not wrapped in functions), the converted script will execute that code on import - ensure all Snowflake tables are created first.

#### 2.2 Synthetic data generation rules

- **Minimal data:** 2-5 rows per table is sufficient for a smoke test
- **Cover key paths:** Include at least one row that matches join conditions
- **Include nulls sparingly:** Only where the workload explicitly handles nulls
- **Use realistic types:** Match the expected schema (dates as ISO strings, decimals with proper precision)

#### 2.3 Schema inference

Infer schema from how data is used in the workload:
- Look at `spark.table("table_name")` calls and trace how columns are used
- Column references: `df.select("col1", "col2")` → columns col1, col2
- Type hints: `.cast("int")`, `IntegerType()` → integer column
- Operations: `.filter(df.amount > 0)` → numeric column
- Joins: `df1.join(df2, "key")` → both have "key" column
- Explicit schemas: If workload defines `StructType([...])`, use those exact columns/types

#### 2.4 Mocking external dependencies

**Tables (Category A):** Use `df.write.mode("overwrite").saveAsTable("<fully_qualified_table_name>")`

**File reads (Category B):** Already handled — external paths were replaced with stage paths in Phase 2.1, and mock data files were uploaded to the stage in Phase 1.3. No additional mocking needed in the entrypoint for these sources.

---

### Phase 3: Run Entrypoint

```bash
cd <workload>_test/
uv run --project <SKILL_DIRECTORY> python entrypoint.py > output.log 2>&1
EXIT_CODE=$?
```

**CRITICAL — On ANY failure** (`EXIT_CODE != 0`):
1. Read `output.log` and include the full error details in the Phase 4 report.
2. **STOP and present the report to the user.** Wait for user reaction before taking any further action.
3. Do NOT attempt to fix the error. Do NOT retry the command. Do NOT modify dependencies, environment, `pyproject.toml`, or virtual environments. Do NOT try alternative package versions or workarounds.

---

### Phase 4: Report

```
════════════════════════════════════════════════════════════
FULL WORKLOAD TEST
════════════════════════════════════════════════════════════

Workload: $ARGUMENTS
Test directory: <workload>_test/
Entrypoint: entrypoint.py

Workload functions called:
- <function_name>(<args>) from <module>
- <function_name>(<args>) from <module>

Mocked tables:
- <fully_qualified_table_name> (N rows): col1, col2, ...
- <fully_qualified_table_name> (N rows): col1, col2, ...

Stage-Mocked External Reads:                        ← omit if none
  Stage: <STAGE_NAME> (preserved for inspection)
  Files:
  - @<STAGE_NAME>/<subpath>/<filename> (N rows): col1, col2, ...
    Original: <original_cloud_path>
  - @<STAGE_NAME>/<subpath>/<filename> (N rows): col1, col2, ...
    Original: <original_cloud_path>

Exit code: <0 or N>

Output:
<show relevant output or errors>

════════════════════════════════════════════════════════════
RESULT: ✅ SUCCESS | ❌ FAILED
════════════════════════════════════════════════════════════
```

**Success criteria:** The workload's main function(s) execute without throwing exceptions. The test verifies that the migrated code is syntactically correct and compatible with SCOS APIs - not that business logic produces correct results.

---

### Phase 5: Cleanup Snowflake Resources

After presenting the Phase 4 report, list the Snowflake resources created during validation and prompt the user. **Do NOT offer to delete the test directory** (`<workload>_test/`) — it must always be preserved.

```
Snowflake resources created during validation:
  - Tables: <fully_qualified_table_name1>, <fully_qualified_table_name2>... (if created by agent)
  - Stage: <STAGE_NAME> (created by agent)             ← omit if stage was user-provided
    Files uploaded:
    - @<STAGE_NAME>/<subpath>/<filename>
    - @<STAGE_NAME>/<subpath>/<filename>

Would you like to clean up these Snowflake resources?
```

If the user accepts, delete in order:

1. **Drop created tables** — only drop tables that the agent explicitly created in Phase 2.
```bash
uv run --project <SKILL_DIRECTORY> python -c "
from snowflake.snowpark import Session
session = Session.builder.config('connection_name', 'default').create()
session.sql('DROP TABLE IF EXISTS <fully_qualified_table_name>').collect()
print('Dropped table: <fully_qualified_table_name>')
"
```

2. **Drop the Snowflake stage** — only if the agent **created** the stage. If the user provided an existing stage in Phase 1.2.1, do **NOT** drop it (only remove the uploaded mock files if desired).
```bash
uv run --project <SKILL_DIRECTORY> python -c "
from snowflake.snowpark import Session
session = Session.builder.config('connection_name', 'default').create()
session.sql('DROP STAGE IF EXISTS <STAGE_NAME>').collect()
print('Dropped stage: <STAGE_NAME>')
"
```

If the user declines, leave all resources in place.

---

## Stopping Points

- ✋ Phase 1.2.1: If external file reads detected — prompt user for stage name before proceeding
- ✋ Phase 1.2.2: If table dependencies detected — check existence and prompt user for permission before proceeding
- ✋ Phase 1: After analyzing workload hierarchy — verify the identified entrypoint and data dependencies are correct before creating test directory
- ✋ Phase 3 (on failure): If the entrypoint run fails, report the error in Phase 4 format and stop. Do NOT attempt to fix, retry, or work around the error. Wait for user reaction.
- ✋ Phase 4: After reporting results — present the report to user
- ✋ Phase 5: After report — list Snowflake resources and ask user whether to clean up (never offer to delete test directory)

## Success Criteria

- The workload's main function(s) execute without throwing exceptions
- The migrated code is syntactically correct and compatible with SCOS APIs
- All Category A data sources are mocked as Snowflake tables or existing tables are used
- All Category B data sources are mocked via Snowflake stage with synthetic files
- The entrypoint calls real workload functions (not reimplemented logic)

## Output

Validation report (Phase 4 format) summarizing pass/fail status, workload functions called, mocked tables, and any errors encountered.

## Troubleshooting

See `references/python/troubleshooting.md` for common issues and solutions.
