# Reporter Critic — Phase 4 Quality Gate

Validate that dashboard reports were correctly generated for the Scala migration.

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
tail -n +2 <CONVERSION_ROOT>/Reports/InputFilesInventory.csv | wc -l
```
Row count should approximately match the manifest `.scala` file count. Zero rows → `FAIL`.

### 4. CSV Structure

Verify Issues.csv has expected columns:
```bash
head -1 <CONVERSION_ROOT>/Reports/Issues.csv
```
Should contain SMA-compatible column names (e.g., `EWI_Code`, `File`, `Line`, `Description`).

### 5. EWI Code Prefix

Verify Issues.csv uses Scala EWI codes (not Python):
```bash
grep -c "SPRKCNTSCL" <CONVERSION_ROOT>/Reports/Issues.csv
```
Should have at least 1 match. If only `SPRKCNTPY` codes found → `FAIL — wrong language prefix`.

## Verdict

- `PASS` — all reports present with valid data and correct EWI prefix
- `FAIL` — [list specific failures]
