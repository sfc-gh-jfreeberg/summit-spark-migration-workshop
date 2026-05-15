# SCOS Fix Rules Reference — Scala

Rules for fixing SCOS compatibility issues found during analysis of Scala workloads. The fixer agent reads this document when applying fixes to migrated files.

**Related references:**
- `references/scala/rdd-conversion.md` — RDD-to-DataFrame conversion rules and examples (required for Rule 2)
- `references/scala/udf-dependencies.md` — UDF serialization fix approach for Scala (required for Rule 10)
- `references/scala/ewi-codes.md` — Official SMA EWI code scheme for Scala (required for `// SCOS:` comment tagging)

---

## Pre-Fix: Read EWI Codes

Before applying fixes, read `references/scala/ewi-codes.md` to understand the official SMA EWI code scheme for Scala. When adding `// SCOS:` comments, include the relevant EWI code where possible. For example:
- `// SCOS: [SPRKCNTSCL1500] RDD operation converted to DataFrame`
- `// SCOS: TODO - [SPRKCNTSCL2500] ML element requires manual migration`

---

## Per-Issue Processing

For EACH issue in `analysis.json`:

1. **Locate the issue**: Find the code at `file` and `lines` in the copied directory.
2. **Assess the risk**: Check the `final_risk` value.
3. **Apply the appropriate action** based on the rules below.
4. **Document the action**: Add a `// SCOS:` comment — **except** for no-op operations and configs (Rules 4 and 5).

---

## Rules for Fixing based on Risk Score

1. **Must Fix (`final_risk` >= 0.7)**: Apply a fix or rewrite. If impossible, add `// SCOS: TODO - <explanation>`.
2. **Should Fix (0.3 <= `final_risk` < 0.7)**: Apply fix if suggested, else `// SCOS: TODO`.
3. **Fix if possible (`final_risk` < 0.3)**: Fix if possible, else `// SCOS: <explanation>`.

---

## General Rules

### Rule 1: Use the Tool's Fix
If the issue provides a `fix` value, use it.

### Rule 2: Handle RDDs
RDD operations (`final_risk` near 1.0) are not supported. Rewrite using DataFrame transformations or SQL. **Read** `references/scala/rdd-conversion.md` for detailed Scala conversion rules.

### Rule 3: Unsupported Formats
Change file formats if required (ORC/Avro → Parquet). Add a downstream impact warning:
```scala
// SCOS: [SPRKCNTSCL1000] ORC format replaced with Parquet — ORC not supported in SCOS
// SCOS: TODO - Verify downstream consumers can accept Parquet instead of ORC
df.write.mode("overwrite").parquet(path)
```

### Rule 4: No-Op Operations
`hint()`, `repartition()`, `coalesce()` are silently ignored in SCOS. Leave as-is, **no comment**.

### Rule 5: No-Op Configs
Unsupported Spark configs (`spark.sql.shuffle.partitions`, `spark.executor.memory`, etc.) are silently ignored. Leave as-is, **no comment**.

### Rule 6: Missing Fixes
If `fix` is null, use `root_cause` for a workaround. If unsure: `// SCOS: TODO - <explanation>`.

### Rule 7: File Reads
Check the path in `.read.csv`, `.read.json`, `.read.parquet`, `.load`:
- **Snowflake stage** (`@STAGE_NAME/...`): No comment needed.
- **Cloud storage** (`s3://`, `gs://`, `abfs://`): Add performance tip recommending stage upload.
- **Local/variable paths**: Add performance tip.

```scala
// SCOS: Performance tip - Consider uploading to a Snowflake stage
val df = spark.read.option("header", "true").csv("s3://bucket/path/file.csv")
```

### Rule 8: Snowflake Connector Pushdown
If code uses `.format("snowflake")` or `.format("net.snowflake.spark.snowflake")`, recommend `SnowflakeSession.sql()`. Keep original code, add comment with suggested replacement.

### Rule 9: Wildcard/Glob File Reads
Wildcard patterns (`*.json`, `*.csv`) in file reads are **not supported**. Replace with explicit file lists:
```scala
// BEFORE (fails in SCOS):
val df = spark.read.json("@MY_STAGE/*.json")

// AFTER:
val df = spark.read.json("@MY_STAGE/file1.json", "@MY_STAGE/file2.json")
```
If exact files unknown: `// SCOS: TODO - [SPRKCNTSCL1000] Wildcard glob not supported`.

### Rule 10: UDF Serialization (Scala)
UDFs referencing custom classes or non-serializable closures may fail. **Read** `references/scala/udf-dependencies.md`.
- **Option 1 (Dev)**: `REPLClassDirMonitor` for compiled class files
- **Option 2 (Prod)**: `spark.addArtifact()` for JAR uploads
- **Option 3**: Staged JARs via `snowpark.connect.udf.java.imports`
- **Inline**: Keep simple UDF logic self-contained in anonymous functions

### Rule 11: StructType in UDFs
In SCOS, `StructType` is converted to `Map` in UDFs instead of `Row`/`tuple`. Rewrite field access from numeric index (`e(0)`) to named access (`e("col1")`).

### Rule 12: checkpoint() Not Supported
Replace `checkpoint()` and `localCheckpoint()` with `cache()`:
```scala
// BEFORE:
df.checkpoint(false)

// AFTER:
// SCOS: [SPRKCNTSCL1000] checkpoint() not supported — replaced with cache()
df.cache()
```

### Rule 13: Scala Version Compatibility
If the workload uses Scala 2.13, add: `spark.conf.set("snowpark.connect.scala.version", "2.13")`. SCOS defaults to 2.12.

### Rule 14: Unsupported Save Modes
`Append` and `Ignore` save modes are not supported for CSV, JSON, Parquet, Text, XML. Replace with `Overwrite` or `ErrorIfExists`:
```scala
// SCOS: [SPRKCNTSCL1000] Append save mode not supported — replaced with overwrite
df.write.mode("overwrite").csv("@STAGE/output")
```

### Rule 15: Spark Catalyst / Internal APIs
Imports from `org.apache.spark.sql.catalyst.*` are not in the Spark Connect client JAR. Create local drop-in case classes:
```scala
// SCOS: [SPRKCNTSCL1000] Catalyst QualifiedTableName replaced with local case class
package com.myproject.model
case class QualifiedTableName(database: String, name: String) {
  override def toString: String = s"$database.$name"
}
```
**⚠️ CRITICAL**: Replace the import in ALL files that reference the type.

### Rule 16: Hadoop / HDFS APIs
`org.apache.hadoop.*` imports are not available. Remove and replace:

| HDFS Operation | SCOS Replacement |
|----------------|-----------------|
| `df.write.parquet(hdfsPath)` | `df.write.saveAsTable("db.table")` or `df.write.parquet("@stage/path")` |
| `spark.read.parquet(hdfsPath)` | `spark.read.table("db.table")` or `spark.read.parquet("@stage/path")` |
| `FileSystem.get(conf).exists(path)` | Remove — Snowflake manages table existence |
| `FileSystem.get(conf).delete(path)` | `spark.sql("DROP TABLE IF EXISTS db.table")` |

Remove `implicit hdfs: FileSystem` from method signatures. **Trace all callers** (Rule 20).

### Rule 16b: Data Lineage Libraries
Remove Spline (`za.co.absa.spline.*`), DataHub, OpenLineage agents. Remove `.enableLineageTracking()`. Snowflake provides native lineage.

### Rule 17: Hive Integration
Remove `enableHiveSupport()`, `HiveContext`, and HWC (`com.hortonworks.spark.sql.hive.*`).

**HWC API → SCOS mapping** (apply to ALL files including tests):

| HWC Call | SCOS Replacement |
|----------|-----------------|
| `hive.sql(query)` | `spark.sql(query)` |
| `hive.executeQuery(query)` | `spark.sql(query)` |
| `hive.table(name)` | `spark.read.table(name)` |
| `hive.session()` | `spark` |
| `hive.setDatabase(db)` | `spark.sql(s"USE $db")` |

**⚠️ CRITICAL**: After removing `implicit val hive: HiveWarehouseSession`, search ALL files for `hive.` references and replace with `spark.sql(...)`.

### Rule 18: Hive DDL Statements
Comment out `MSCK REPAIR TABLE`, `ALTER TABLE RECOVER PARTITIONS`, `CREATE EXTERNAL TABLE`:
```scala
// SCOS: TODO - [SPRKCNTSCL1000] MSCK REPAIR TABLE is Hive-specific.
// Snowflake manages partitions automatically.
// spark.sql("MSCK REPAIR TABLE schema.table")
```

### Rule 19: External Library Parameter Mismatch
After removing parameters (e.g., `hdfs: FileSystem`), check if external library calls still expect them. Add TODO if so.

### Rule 20: ⚠️ Cross-File Consistency (MANDATORY)
When you modify a method signature, remove a method/parameter/variable, or change a type:
1. Grep the **entire codebase** (including tests) for references
2. Update **every caller** to match the new signature
3. Update every subclass/implementation
4. Verify the call chain (callers of callers)
5. Check variable references (`hive.` → `spark.sql(...)`)

```bash
# After removing hdfs parameter:
grep -rn "Job\.run" <MIGRATED>/ --include="*.scala"
# After removing HWC variable:
grep -rn "hive\." <MIGRATED>/ --include="*.scala"
# After replacing a Catalyst type:
grep -rn "QualifiedTableName" <MIGRATED>/ --include="*.scala"
```

**Failure to do this is the #1 cause of compilation errors.**

### Rule 21: ⚠️ Import Replacement Emission (MANDATORY)
Only emit syntactically valid Scala import lines. **NEVER** append text, em-dashes, or descriptions after the import path:

**Correct:**
```scala
// SCOS: [SPRKCNTSCL1000] Removed: import org.apache.hadoop.fs.FileSystem
import com.myproject.model.QualifiedTableName
```

**INVALID (causes compilation error):**
```scala
import com.myproject.model.QualifiedTableName — replaced with local model class
```

### Rule 22: ⚠️ Syntax Artifact Cleanup (MANDATORY)
After ALL edits, scan for malformed lines:
```bash
grep -rn '^import .*[—–]' <MIGRATED>/ --include="*.scala"
grep -rn '^—\|^[[:space:]]*—[[:space:]]*$' <MIGRATED>/ --include="*.scala"
```
Fix: move trailing text to comment lines, delete bare em-dash lines.

### Rule 23: Map Column Subscript with Column Key
`mapCol(col("key"))` is not supported. Replace with `element_at()`:
```scala
// BEFORE:
val result = df.withColumn("val", categoryMap(col("category_code")))

// AFTER:
// SCOS: [SPRKCNTSCL1000] Map column subscript replaced with element_at()
import org.apache.spark.sql.functions.element_at
val result = df.withColumn("val", element_at(categoryMap, col("category_code")))
```
Literal keys (`mapCol("literal_string")`) still work.
