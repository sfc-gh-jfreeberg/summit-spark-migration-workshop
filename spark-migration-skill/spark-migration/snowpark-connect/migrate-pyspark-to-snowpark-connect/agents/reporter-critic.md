# Reporter Critic — Phase 4 Quality Gate

Validate that dashboard reports were correctly generated.

## Inputs

- `migration_state.json` for manifest file count and conversion root
- Report files in `<CONVERSION_ROOT>/Reports/`

## Checks

### 1. Required Files Exist

```bash
test -f <CONVERSION_ROOT>/Reports/Issues.csv && echo "OK Issues.csv" || echo "MISSING Issues.csv"
test -f <CONVERSION_ROOT>/Reports/InputFilesInventory.csv && echo "OK InputFilesInventory.csv" || echo "MISSING InputFilesInventory.csv"
test -f <CONVERSION_ROOT>/Reports/ArtifactDependencyInventory.csv && echo "OK ArtifactDependencyInventory.csv" || echo "MISSING"
```
All three must exist. Any missing → `FAIL`.

### 2. Issues.csv Has Data

```bash
wc -l < <CONVERSION_ROOT>/Reports/Issues.csv
```
Must have >1 line (header + at least 1 data row). If only header or empty → `FAIL — Issues.csv has no data rows`.

### 3. InputFilesInventory.csv Row Count

```bash
# Data rows (excluding header)
tail -n +2 <CONVERSION_ROOT>/Reports/InputFilesInventory.csv | wc -l
```
Row count should approximately match the manifest file count from `migration_state.json`. Exact match not required (some non-PySpark files may be excluded), but zero rows → `FAIL`.

### 4. CSV Structure

Verify Issues.csv has expected columns:
```bash
head -1 <CONVERSION_ROOT>/Reports/Issues.csv
```
Should contain columns like `EWI_Code`, `File`, `Line`, `Description` (or similar SMA-compatible column names).

## Verdict

- `PASS` — all reports present with valid data
- `FAIL` — [list specific failures]
