# Import Updater Agent — Phase 3 Specialist

Update imports, session initialization, build configuration, and add migration headers to every Scala file.

## Inputs

Read `migration_state.json` to get:
- `manifest` — list of ALL `.scala` files to process
- `build_files` — list of build files (pom.xml, build.sbt, etc.)
- `migrated_dir` — directory with source files

## Step 1: Update Session Initialization

Identify the **main entry point** (the file with `SparkSession.builder` and `def main` or `extends App`).

**In the entry point ONLY**, convert to Spark Connect remote:
```scala
// BEFORE:
val spark = SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()

// AFTER:
// SCOS: [SPRKCNTSCL3500] Converted to Spark Connect remote session
val remoteUrl = sys.env.getOrElse("SPARK_REMOTE",
  sparkConf.get("spark.remote", "sc://localhost:15002"))
val builder = SparkSession.builder().remote(remoteUrl)
sparkConf.getAll.foreach { case (k, v) =>
  if (!k.startsWith("hive.") && !k.startsWith("spark.sql.hive."))
    builder.config(k, v)
}
val spark = builder.getOrCreate()
```

**In test files** (`*Spec.scala`, `*Test.scala`, `*Suite.scala`): Keep `master("local[*]")`. Do NOT use `.remote()`. Add a TODO noting SCOS conversion is needed for integration testing.

**In all other files**: Remove redundant session creation. Ensure the file uses the session passed as a parameter.

## Step 2: Remove Unsupported Imports

For **EACH** `.scala` file in the manifest, remove:

| Unsupported Import | Action |
|---|---|
| `org.apache.spark.graphx.*` | Remove |
| `org.apache.spark.streaming.*` | Remove |
| `org.apache.spark.mllib.*` | Remove |
| `org.apache.spark.ml.*` | Remove |
| `org.apache.spark.sql.catalyst.*` | Remove — replace with local case class per Rule 15 |
| `org.apache.spark.sql.hive.*` | Remove |
| `org.apache.hadoop.*` | Remove |
| `com.hortonworks.spark.sql.hive.*` | Remove — replace HWC calls with spark.sql() |
| `za.co.absa.spline.*` | Remove |
| `delta.tables.*` | Remove |
| Internal logging libs | Replace with `org.slf4j.{Logger, LoggerFactory}` |

**⚠️ Clean removal**: Delete entire lines. Do NOT leave trailing text, em-dashes, or fragments on import lines (Rule 21). Standard Spark SQL imports (`org.apache.spark.sql.functions._`, `org.apache.spark.sql.types._`, etc.) are supported — do NOT remove them.

## Step 3: Update Build Configuration

**Actively transform** the build file — do NOT just add TODO comments.

| Action | sbt | Maven |
|--------|-----|-------|
| Scala version → 2.12.18 | `scalaVersion := "2.12.18"` | `<scala.version>2.12.18</scala.version>` |
| Spark version → 3.5.6 | Update in libraryDependencies | `<spark.version>3.5.6</spark.version>` |
| Java → 11 | `javacOptions ++= Seq("-source", "11", "-target", "11")` | `<maven.compiler.source>11</maven.compiler.source>` |
| Add spark-connect-client-jvm | `"org.apache.spark" %% "spark-connect-client-jvm" % sparkVersion` | Add `<dependency>` block |
| Remove spark-hive, HWC, Spline | Delete entries | Delete `<dependency>` blocks |
| Update _2.11 suffixes | Use `%%` | Change to `_${scala.short}` |
| Bump cross-compiled lib versions | Per ecosystem table | Per ecosystem table |
| Add --add-opens JVM flags (Java 11+) | `Test / javaOptions ++= Seq(...)` | Add to plugin `<configuration>` |

## Step 4: Add Migration Header

Add a block comment at the **top** of EVERY `.scala` file:

```scala
/*
 * SCOS Migration Output
 * =====================
 * Source File: [original file path]
 * Migrated on: [current date]
 *
 * Changes Overview:
 * - [Lines X-Y] Description of change
 *
 * Known Limitations:
 * - [List every // SCOS: TODO item, or "None — all issues resolved"]
 */
```

For files with no changes: use "No compatibility issues detected. No changes required."

## Step 5: Track Progress

Process files one at a time. After each file, update `migration_state.json`:
```json
"3_imports": {"files_done": ["File1.scala"], "files_remaining": ["File2.scala"]}
```

## Completeness Gate

Before reporting done:
- [ ] Every `.scala` file in manifest has been processed
- [ ] Each file has a migration header block comment
- [ ] Entry point uses `.remote()` for Spark Connect
- [ ] Test files keep `master("local[*]")` — NOT `.remote()`
- [ ] No unsupported imports remain
- [ ] Build file actively transformed (versions, deps, suffixes)
- [ ] File count matches: manifest count == processed count

Report: "Imports updated: X files processed, session init replaced in <entry_point>, build file transformed"

## Output

- Modified files with updated imports and headers
- Modified build file(s)
- Updated `migration_state.json` with phase 3 status
