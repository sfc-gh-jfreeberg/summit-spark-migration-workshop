# Import Critic — Phase 3 Quality Gate

Validate that all imports, session init, build files, and migration headers are correct for Scala.

## Inputs

- `migration_state.json` for the file manifest
- Modified source files in `<MIGRATED>/`

## Checks

### 1. Migration Header Present

Every `.scala` file in the manifest must have a migration header:
```bash
for f in $(find <MIGRATED> -name "*.scala" -type f | sort); do
  if head -10 "$f" | grep -q "SCOS Migration"; then
    echo "OK $f"
  else
    echo "MISSING $f"
  fi
done
```
Any `MISSING` → `FAIL`.

### 2. Session Init Replaced

Scan all NON-TEST files for `enableHiveSupport()` or `.master("yarn")` in non-comment code:
```bash
grep -rn "enableHiveSupport\|\.master(\"yarn\")\|\.master(\"local" <MIGRATED>/ --include="*.scala" | grep -v "Spec\.scala\|Test\.scala\|Suite\.scala\|src/test/" | grep -v "^.*//\|^\s*/\*\|^\s*\*"
```
If any matches found in production code (not comments/not tests) → `FAIL`.

### 3. Spark Connect Init Present

The entry point file must contain `.remote(`:
```bash
grep -rln "\.remote(" <MIGRATED>/ --include="*.scala" | grep -v "Spec\.scala\|Test\.scala\|Suite\.scala"
```
At least one non-test file must match. If zero → `FAIL — no file has Spark Connect remote initialization`.

### 4. No Unsupported Imports

```bash
grep -rn "org.apache.spark.sql.catalyst\|org.apache.hadoop\|com.hortonworks\|za.co.absa.spline\|org.apache.spark.sql.hive\|delta.tables" <MIGRATED>/ --include="*.scala" | grep -v "^.*//\|^\s*/\*\|^\s*\*"
```
Any matches in non-comment code → `FAIL — unsupported import remains at <file>:<line>`.

### 5. Build File Verification

```bash
if [ -f <MIGRATED>/pom.xml ]; then
  grep -q "spark-connect-client-jvm" <MIGRATED>/pom.xml && echo "OK spark-connect" || echo "MISSING spark-connect"
  grep -q "2\.11" <MIGRATED>/pom.xml && echo "FAIL still has 2.11" || echo "OK no 2.11"
fi
if [ -f <MIGRATED>/build.sbt ]; then
  grep -q "spark-connect-client-jvm" <MIGRATED>/build.sbt && echo "OK spark-connect" || echo "MISSING spark-connect"
fi
```
Missing spark-connect-client-jvm → `FAIL`. Remaining 2.11 references → `FAIL`.

### 6. Syntax Artifact Check (Rules 21-22)

```bash
grep -rn '^import .*[—–]' <MIGRATED>/ --include="*.scala"
grep -rn '^import .* removed' <MIGRATED>/ --include="*.scala"
grep -rn '^—\|^[[:space:]]*—[[:space:]]*$' <MIGRATED>/ --include="*.scala"
```
Any matches → `FAIL — syntax artifacts from editing`.

### 7. File Count

Manifest count must equal current `.scala` file count. No files created or deleted.

## Verdict

- `PASS` — all headers present, imports clean, session replaced, build transformed
- `FAIL` — [list specific failures]
