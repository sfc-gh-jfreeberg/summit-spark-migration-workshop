# Fixer Critic — Phase 2 Quality Gate

Validate that all fixes were correctly applied to Scala files before proceeding to import updates.

## Inputs

- `analysis.json` in the conversion root
- `migration_state.json` for the file manifest
- Modified source files in `<MIGRATED>/`

## Checks

### 1. Compilation Validation (Zero Tolerance)

If the project has a build tool (sbt, Maven):
```bash
# sbt project:
cd <MIGRATED> && sbt compile 2>&1

# Maven project:
cd <MIGRATED> && mvn compile -q 2>&1
```

If no build tool is available, check for basic Scala syntax issues:
```bash
# Check for import lines with trailing text (never valid Scala — Rule 21)
grep -rn '^import .*[—–]' <MIGRATED>/ --include="*.scala"
grep -rn '^import .* removed' <MIGRATED>/ --include="*.scala"

# Check for bare em-dash lines (never valid Scala — Rule 22)
grep -rn '^—\|^[[:space:]]*—[[:space:]]*$' <MIGRATED>/ --include="*.scala"
```
Any syntax error or malformed line → `FAIL`. Report the exact file and error.

### 2. High-Risk Issue Coverage

For each issue in `analysis.json` with `final_risk >= 0.7`:
1. Read the file at the reported location
2. Verify ONE of:
   - Code was changed (fix applied)
   - A `// SCOS:` or `// SCOS: TODO` comment exists at or near that line
   - The issue was in code that was removed/rewritten

If a high-risk issue has no fix AND no comment → `FAIL`.

### 3. No-Op Over-Annotation Check

Scan all files for `// SCOS:` comments near these patterns:
- `.hint(` — should have NO comment (no-op, leave as-is)
- `.repartition(` — should have NO comment (no-op, leave as-is)
- `.coalesce(` — should have NO comment (no-op, leave as-is)
- No-op Spark configs — should have NO comment

If any no-op has a `// SCOS:` annotation → `FAIL — over-annotated no-op at <file>:<line>`.

**Exception**: `.coalesce()` and `.repartition()` used in `.rdd.` chains ARE problems (RDD access) and SHOULD be annotated.

### 4. Cross-File Consistency (Rule 20)

```bash
# Check for references to removed Hadoop/Hive/Catalyst types in non-comment code
grep -rn "FileSystem\|hadoopConfiguration\|HiveContext\|enableHiveSupport\|QualifiedTableName\|TableIdentifier\|CatalystSqlParser" <MIGRATED>/ --include="*.scala" | grep -v "^.*//\|^\s*/\*\|^\s*\*"

# Check for HWC variable references
grep -rn "hive\.\|HiveWarehouseSession\|getHWCSession\|getHiveSession" <MIGRATED>/ --include="*.scala" | grep -v "^.*//\|^\s*/\*\|^\s*\*"
```
If stale references found in non-comment code → `FAIL — broken cross-file reference`.

### 5. File Integrity

```bash
echo "Manifest: $(find <MIGRATED> -name '*.scala' -type f | wc -l) files"
```
Count must match manifest. No file should have been accidentally deleted or created.

### 6. Git Diff Sanity

```bash
cd <CONVERSION> && git diff --stat HEAD
```
- Every changed file should be in the manifest
- No file should be empty (0 bytes)

## Verdict

- `PASS` — all checks green
- `FAIL` — [list specific failures with file:line references]

If `FAIL`, provide exact instructions: which files need re-fixing, what was wrong, and the expected fix.
