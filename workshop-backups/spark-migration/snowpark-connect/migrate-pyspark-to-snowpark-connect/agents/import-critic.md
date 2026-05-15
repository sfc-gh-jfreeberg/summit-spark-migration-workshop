# Import Critic — Phase 3 Quality Gate

Validate that all imports, session init, and migration headers are correct.

## Inputs

- `migration_state.json` for the file manifest
- Modified source files in `<MIGRATED>/`

## Checks

### 1. Migration Header Present

Every `.py` file in the manifest must have a migration header:
```bash
for f in $(find <MIGRATED> -name "*.py" -type f | sort); do
  if head -10 "$f" | grep -q "SCOS Migration"; then
    echo "OK $f"
  else
    echo "MISSING $f"
  fi
done
```
Any `MISSING` → `FAIL`.

### 2. Session Init Replaced

Scan all files for `SparkSession.builder` in non-comment, non-docstring code:
```bash
grep -rn "SparkSession.builder" <MIGRATED>/*.py | grep -v "^.*#" | grep -v '"""' | grep -v "'''"
```
If any matches found in actual code (not comments/docstrings) → `FAIL`.

### 3. Snowpark Connect Init Present

The entry point file must contain `snowpark_connect`:
```bash
grep -l "snowpark_connect" <MIGRATED>/*.py
```
At least one file must match. If zero → `FAIL — no file has snowpark_connect initialization`.

### 4. No Unsupported Imports

```bash
grep -rn "from databricks" <MIGRATED>/*.py
grep -rn "from delta.tables" <MIGRATED>/*.py
```
Any matches → `FAIL — unsupported import remains at <file>:<line>`.

### 5. File Count

Manifest count must equal current `.py` file count. No files created or deleted.

## Verdict

- `PASS` — all headers present, imports clean, session replaced
- `FAIL` — [list specific failures]
