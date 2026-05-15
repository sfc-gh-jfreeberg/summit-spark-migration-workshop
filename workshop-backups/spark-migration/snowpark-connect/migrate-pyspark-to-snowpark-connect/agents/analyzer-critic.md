# Analyzer Critic — Phase 1 Quality Gate

Validate the analysis output before the fixer phase begins.

## Inputs

- `analysis.json` in the conversion root
- `migration_state.json` for the file manifest
- Source files in `<MIGRATED>/`

## Checks

### 1. Valid JSON
```bash
python3 -c "import json; json.load(open('analysis.json')); print('VALID')"
```
Must print `VALID`. If not: `FAIL — analysis.json is not valid JSON`.

### 2. File Coverage

Every `.py` file in the manifest should appear in at least one `analysis.json` entry OR have been scanned (even if no issues found). Check:
```bash
# Files in manifest
find <MIGRATED> -name "*.py" -type f | sort > /tmp/manifest.txt
# Files in analysis
python3 -c "import json; files=set(e['file'] for e in json.load(open('analysis.json'))); [print(f) for f in sorted(files)]" > /tmp/analyzed.txt
```

If files are missing from analysis AND contain PySpark code (`import pyspark`), that's a gap.

### 3. Snowpark Connect Import Requirement

Verify that `analysis.json` entries account for the fact that Snowpark Connect requires an explicit `from snowflake import snowpark_connect` import. This is a current limitation that will be removed in the future, but is needed as of today. If no analysis entry flags missing snowpark-connect imports, note this for the import-updater phase.

### 4. Known Blind Spot Scan

Scan ALL source files for patterns the analyzer historically misses:

| Pattern | Grep command |
|---------|-------------|
| `@udf` decorator | `grep -rn "@udf" <MIGRATED>/*.py` |
| `@pandas_udf` decorator | `grep -rn "@pandas_udf" <MIGRATED>/*.py` |
| `applyInPandas` | `grep -rn "applyInPandas" <MIGRATED>/*.py` |
| `checkpoint()` | `grep -rn "\.checkpoint(" <MIGRATED>/*.py` |
| Map subscript with col | `grep -rn "\[col(" <MIGRATED>/*.py` |
| SparkContext access | `grep -rn "sparkContext" <MIGRATED>/*.py` |
| Hadoop filesystem | `grep -rn "hadoop\|hdfs://" <MIGRATED>/*.py` |
| USE DATABASE/SCHEMA | `grep -rn "USE DATABASE\|USE SCHEMA" <MIGRATED>/*.py` |
| Deequ/pydeequ | `grep -rn "deequ\|pydeequ" <MIGRATED>/*.py` |
| Delta Lake | `grep -rn "delta\.tables\|DeltaTable\|format.*delta" <MIGRATED>/*.py` |
| ML pipeline | `grep -rn "VectorAssembler\|Pipeline\|CrossValidator" <MIGRATED>/*.py` |
| UDTF decorator | `grep -rn "@udtf" <MIGRATED>/*.py` |

For each found pattern, check if `analysis.json` already has an entry for that file+line. If not, this is a gap.

### 5. Risk Distribution Sanity

- If analysis has 0 issues but source contains PySpark imports → suspicious, likely analyzer failure
- If all issues have `final_risk < 0.1` but source has `sparkContext`, `.rdd`, or `broadcast` → likely false negatives

## Verdict

Report one of:
- `PASS` — analysis is complete and covers all files and known patterns
- `PASS_WITH_GAPS` — analysis is mostly complete but N supplementary entries were needed (already added by analyzer)
- `FAIL` — critical gaps found: [list specific files/patterns missed]

If `FAIL`, provide specific instructions for what the analyzer should re-scan.
