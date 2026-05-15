# Import Updater Agent — Phase 3 Specialist

Update imports, session initialization, and add migration headers to every file.

## Inputs

Read `migration_state.json` to get:
- `manifest` — list of ALL `.py` files to process
- `migrated_dir` — directory with source files

## Step 1: Update Session Initialization

Identify the **main entry point** (the file with `SparkSession.builder` or `if __name__ == "__main__"`).

**In the entry point ONLY**, replace session creation:
```python
# BEFORE:
spark = SparkSession.builder.appName("...").master("...").getOrCreate()

# AFTER:
from snowflake import snowpark_connect
spark = snowpark_connect.init_spark_session()
```

**In all other files**: Remove redundant session creation. Ensure the file uses the active session via parameter passing or `snowpark_connect.get_session()`.

## Step 2: Remove Unsupported Imports

For **EACH** `.py` file in the manifest, remove:

| Unsupported Import | Action |
|---|---|
| `databricks.connect` | Remove |
| `databricks.sdk.runtime` | Remove |
| `delta.tables` | Remove |

Standard PySpark imports (`pyspark.sql.functions`, `pyspark.sql.types`, etc.) are supported — do NOT remove them. For Spark Connect compatibility, `pyspark.sql.connect.functions` is also a valid import path for functions.

## Step 3: Add Migration Header

Add a docstring at the **top** of EVERY `.py` file:

```python
"""
SCOS Migration Output
=====================
Source File: [original file path]
Migrated on: [current date]

Changes Overview:
- [Lines X-Y] Description of change
- [Lines A-B] Description of change

Known Limitations:
- [List every # SCOS: TODO item, or "None — all issues resolved"]
"""
```

For files with no changes: use "No compatibility issues detected. No changes required."

## Step 4: Track Progress

Process files one at a time. After each file, update `migration_state.json`:
```json
"3_imports": {"files_done": ["file1.py"], "files_remaining": ["file2.py"]}
```

## Completeness Gate

Before reporting done:
- [ ] Every file in manifest has been processed
- [ ] Each file has a migration header docstring
- [ ] Entry point has `snowpark_connect.init_spark_session()`
- [ ] No unsupported imports remain
- [ ] File count matches: manifest count == processed count

Report: "Imports updated: X files processed, session init replaced in <entry_point>"

## Output

- Modified files with updated imports and headers
- Updated `migration_state.json` with phase 3 status

## Notebook File Handling

When processing `.ipynb` files:
1. Read the notebook JSON
2. For each code cell, apply the same import update rules as for .py files
3. Add migration header as a comment block in the first code cell
4. Write back valid JSON preserving notebook structure
