# Analyzer Critic — Phase 1 Quality Gate

Validate the Scala analysis output before the fixer phase begins.

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

Every `.scala` file in the manifest should appear in at least one `analysis.json` entry OR have been scanned (even if no issues found). Check:
```bash
# Files in manifest
find <MIGRATED> -name "*.scala" -type f | sort > /tmp/manifest.txt
# Files in analysis
python3 -c "import json; files=set(e['file'] for e in json.load(open('analysis.json'))); [print(f) for f in sorted(files)]" > /tmp/analyzed.txt
```

If files are missing from analysis AND contain Spark imports (`import org.apache.spark`), that's a gap.

### 3. Known Blind Spot Scan

Scan ALL source files for patterns the analyzer historically misses:

| Pattern | Grep command |
|---------|-------------|
| UDF registration | `grep -rn "udf(\|spark.udf.register\|UserDefinedFunction" <MIGRATED>/ --include="*.scala"` |
| Checkpoint | `grep -rn "\.checkpoint(" <MIGRATED>/ --include="*.scala"` |
| Map subscript with col | `grep -rn "(col(" <MIGRATED>/ --include="*.scala"` |
| Catalyst imports | `grep -rn "spark.sql.catalyst" <MIGRATED>/ --include="*.scala"` |
| Hadoop imports | `grep -rn "org.apache.hadoop" <MIGRATED>/ --include="*.scala"` |
| HWC imports | `grep -rn "HiveWarehouseSession\|com.hortonworks" <MIGRATED>/ --include="*.scala"` |
| Spline imports | `grep -rn "za.co.absa.spline" <MIGRATED>/ --include="*.scala"` |

For each found pattern, check if `analysis.json` already has an entry for that file+line. If not, this is a gap.

### 4. Risk Distribution Sanity

- If analysis has 0 issues but source contains Spark imports → suspicious, likely analyzer failure
- If all issues have `final_risk < 0.1` but source has `sparkContext`, `.rdd`, `HiveWarehouseSession`, or Catalyst imports → likely false negatives

## Verdict

Report one of:
- `PASS` — analysis is complete and covers all files and known patterns
- `PASS_WITH_GAPS` — analysis is mostly complete but N supplementary entries were needed (already added by analyzer)
- `FAIL` — critical gaps found: [list specific files/patterns missed]

If `FAIL`, provide specific instructions for what the analyzer should re-scan.
