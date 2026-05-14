# Fixer Critic — Phase 2 Quality Gate

Validate that all Snowpark Connect compatibility fixes were correctly applied before proceeding to import updates.

## Inputs

- `analysis.json` in the conversion root
- `migration_state.json` for the file manifest
- Modified source files in `<MIGRATED>/`

## Checks

### 1. Syntax Validation (Zero Tolerance)

Every `.py` file in the manifest must pass:
```bash
find <MIGRATED> -name "*.py" -exec python3 -m py_compile {} \;
```
Any syntax error → `FAIL`. Report the exact file and error.

For each failing file, capture the exact error:
```bash
python3 -c "import py_compile; py_compile.compile('<file>', doraise=True)"
```
Include the file path and exact error message in the FAIL report so the fixer can target the specific syntax issue on retry.

### 1a. Notebook JSON Validation

Every `.ipynb` file in the manifest must be valid JSON with correct notebook structure:
```bash
for nb in $(find <MIGRATED> -name "*.ipynb" -type f); do
  python3 -c "
import json, sys
nb = json.load(open('$nb'))
assert 'cells' in nb, 'Missing cells key'
for cell in nb['cells']:
    assert 'cell_type' in cell and 'source' in cell, f'Bad cell structure in $nb'
print('OK: $nb')
" || echo "INVALID_NOTEBOOK: $nb"
done
```
Any invalid notebook → `FAIL`.

Verify markdown cells are unchanged from the source copy (compare with initial commit).

### 2. High-Risk Issue Coverage

For each issue in `analysis.json` with `final_risk >= 0.7`:
1. Read the file at the reported location
2. Verify ONE of:
   - Code was changed (fix applied)
   - A `# SCOS:` or `# SCOS: TODO` comment exists at or near that line
   - The issue was in code that was removed/rewritten

If a high-risk issue has no fix AND no comment → `FAIL`.

### 3. No-Op Over-Annotation Check

Scan all files for `# SCOS:` comments near these patterns:
- `.hint(` — should have NO comment (no-op, leave as-is)
- `.repartition(` — should have NO comment (no-op, leave as-is)
- `.coalesce(` — should have NO comment (no-op, leave as-is)
- No-op Spark configs (`spark.sql.shuffle.partitions`, `spark.executor.memory`, `spark.driver.memory`, etc.) — should have NO comment

If any no-op has a `# SCOS:` annotation → `FAIL — over-annotated no-op at <file>:<line>`.

**Exception**: `.coalesce()` and `.repartition()` used in `.rdd.` chains ARE problems (RDD access) and SHOULD be annotated. Only flag over-annotation when the no-op is on a DataFrame method call.

### 4. File Integrity

```bash
# Count original vs current
echo "Manifest: $(wc -l < /tmp/manifest.txt) files"
echo "Current:  $(find <MIGRATED> -name '*.py' -type f | wc -l) files"
```
Counts must match. No file should have been accidentally deleted or created.

### 5. Git Diff Sanity

```bash
cd <CONVERSION> && git diff --stat HEAD
```
- Every changed file should be in the manifest
- No unexpected files modified (e.g., `migration_state.json` changes are OK)
- No file should be empty (0 bytes)

## Verdict

- `PASS` — all checks green
- `FAIL` — [list specific failures with file:line references]

If `FAIL`, provide exact instructions: which files need re-fixing, what was wrong, and the expected fix.
