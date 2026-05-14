# SCOS Migration & Validation Troubleshooting — Scala

Common issues and solutions for migrating and validating Spark Scala workloads with Snowpark Connect (SCOS).

---

## Setup & Environment Issues

### Error: uv not found

**Cause:** The `uv` package manager is not installed (needed for running the analyzer).

**Fix:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
Restart your terminal after installation.

---

### Error: Snowflake connection failed

**Cause:** The Snowflake connection is not configured or credentials are invalid.

**Fix:**
- Verify the `default` connection is configured (or use `--connection <name>`)
- Check credentials and network connectivity
- Ensure your Snowflake account is accessible

---

### Error: RAG resources exist but access denied

**Cause:** The RAG knowledge base was set up by another user and you don't have access.

**Fix:** Ask your Snowflake admin to grant access:
```sql
GRANT USAGE ON DATABASE SCOS_MIGRATION TO ROLE <your_role>;
GRANT USAGE ON SCHEMA SCOS_MIGRATION.PUBLIC TO ROLE <your_role>;
GRANT SELECT ON TABLE SCOS_MIGRATION.PUBLIC.SCOS_COMPAT_ISSUES TO ROLE <your_role>;
GRANT USAGE ON CORTEX SEARCH SERVICE SCOS_MIGRATION.PUBLIC.SCOS_COMPAT_ISSUES_SERVICE TO ROLE <your_role>;
```

---

## Migration Issues

### Error: Analysis returns empty results

**Cause:** The path doesn't contain Spark Scala code or `.scala` files.

**Fix:**
- Verify the path contains `.scala` files
- Check if files contain Spark code (imports from `org.apache.spark`)

---

### Error: Compilation fails after migration

**Cause:** Incomplete edits or malformed code introduced during migration.

**Fix:**
- Review the specific file for incomplete edits
- Check for mismatched brackets or unclosed string literals
- Run `scalac <file>` or use `sbt compile` to identify the exact compilation error
- Verify import statements are syntactically valid

---

### Error: Import errors after migration

**Cause:** Unsupported imports remain or Snowpark Connect session initialization is incorrect.

**Fix:**
- Ensure unsupported imports (`org.apache.spark.graphx`, `delta`) are removed
- Verify Snowpark Connect session initialization is correct:
  ```scala
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.connect.client.REPLClassDirMonitor

  val spark = SparkSession.builder()
    .remote("sc://localhost:15002")
    .getOrCreate()
  ```

---

### Error: Scala version mismatch

**Cause:** The workload is built with Scala 2.13 but Snowpark Connect defaults to 2.12.

**Fix:** Set the Scala version configuration:
```scala
val spark = SparkSession.builder()
  .remote("sc://localhost:15002")
  .config("snowpark.connect.scala.version", "2.13")
  .getOrCreate()
```

Or via session config:
```scala
spark.conf.set("snowpark.connect.scala.version", "2.13")
```

---

## Validation Issues

### ImportError / ClassNotFoundException

**Cause:** The workload class or module cannot be found on the classpath.

**Fix:** Ensure:
- The compiled `.class` files are on the classpath
- `build.sbt` includes the `spark-connect-client-jvm` dependency:
  ```scala
  libraryDependencies += "org.apache.spark" %% "spark-connect-client-jvm" % "3.5.6"
  ```
- JVM options include module compatibility flags:
  ```scala
  javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED")
  ```

---

### UDF class not found on server

**Cause:** UDF or custom code references classes not available on Snowflake's server-side worker.

**Fix:** Apply the approach from `references/scala/udf-dependencies.md`:
- **Option 1 (Preferred):** Register a `REPLClassDirMonitor` to monitor and upload class files
- **Option 2:** Upload JAR dependencies via `spark.addArtifact()`
- **Option 3:** Use staged JARs via `snowpark.connect.udf.java.imports`

---

### Schema mismatch at runtime

**Cause:** Synthetic data schema doesn't match what the workload expects.

**Fix:** Re-check column names and types used downstream. In Scala, pay attention to:
- Implicit type conversions (ByteType/ShortType/IntegerType → LongType in SCOS)
- StructType in UDFs (SCOS returns `dict` / `Map` instead of `tuple` / `Row`)
- NullType is inferred as StringType in SCOS

---

### Stage creation fails

**Cause:** Warehouse is inactive or user lacks permissions.

**Fix:**
- Verify the warehouse is active: `ALTER WAREHOUSE <name> RESUME`
- Verify the user has `CREATE STAGE` privilege
- Check that the database and schema in the active session are accessible

---

### spark.read with wildcard pattern fails (AssertionError, ERROR CODE: 5001)

**Cause:** Wildcard/glob patterns (`*.json`, `*.csv`, `*.parquet`) in file read paths are not supported in SCOS.

**Example error:**
```
SparkConnectGrpcException: AssertionError (ERROR CODE: 5001)
```

Triggered by code like:
```scala
val df = spark.read.json("@MY_STAGE/*.json")
```

**Fix:** Replace wildcard reads with explicit file lists:
```scala
// BEFORE (fails):
val df = spark.read.json("@MY_STAGE/*.json")

// AFTER (works):
val df = spark.read.json(
  "@MY_STAGE/file1.json",
  "@MY_STAGE/file2.json",
  "@MY_STAGE/file3.json"
)
```

---

### Error: UNSUPPORTED_DATA_TYPE on map column subscript

**Cause:** Using `mapCol(col("key"))` (apply-style indexing) to index a map column with another `Column` as the key. In Spark Connect, `Column.apply` only accepts literal values, not `Column` expressions.

**Example error:**
```
UNSUPPORTED_DATA_TYPE: Unsupported DataType 'Column'
```

Triggered by code like:
```scala
val categoryMap = map(lit("A"), lit(1), lit("B"), lit(2))
val result = df.withColumn("val", categoryMap(col("category_code")))
```

**Fix:** Replace apply-style indexing with `element_at()`, which accepts `Column` arguments and works in both classic and Connect modes:
```scala
import org.apache.spark.sql.functions.element_at

// BEFORE (fails in Connect):
val result = df.withColumn("val", categoryMap(col("category_code")))

// AFTER (works in both):
val result = df.withColumn("val", element_at(categoryMap, col("category_code")))
```

Apply-style indexing with **literal** keys (e.g., `mapCol("some_string")`) still works.

---

## UDF & Serialization Issues

### ClassNotFoundException for UDF classes

**Cause:** Spark Connect serializes UDF closures and sends them to the Snowflake server. If the closure references classes not available on the server, it throws ClassNotFoundException.

**Fix:** Register a class finder or upload dependencies:

```scala
// Option 1: REPLClassDirMonitor
import org.apache.spark.sql.connect.client.REPLClassDirMonitor
val classFinder = new REPLClassDirMonitor("/absolute/path/to/target/scala-2.12/classes")
spark.registerClassFinder(classFinder)

// Option 2: Upload JAR
spark.addArtifact("/absolute/path/to/dependency.jar")

// Option 3: Staged JAR
spark.conf.set("snowpark.connect.udf.java.imports",
  "[@mystage/dependency.jar, @db.schema.stage/other_dependency.jar]")
```

---

### StructType differences in UDFs

**Cause:** Snowpark Connect converts StructType to `dict`/`Map` in UDFs, not `tuple`/`Row` like native Spark.

**Fix:** Access struct fields by name (`"_1"`, `"_2"`, or field names) instead of numeric index:
```scala
// BEFORE (Spark): e(0)
// AFTER (SCOS): e("_1") or e("col1")
```

---

### Iterator type not supported in UDFs

**Cause:** `Iterator` is not supported as an input or return type in SCOS UDFs.

**Fix:** Rewrite to use non-iterator patterns:
```scala
// BEFORE (not supported):
def func(iterator: Iterator[Row]): Iterator[Row] = { ... }

// AFTER: Use mapInPandas or applyInPandas patterns instead, or
// restructure to work on individual rows/batches
```

---

## Data Source Issues

### Unsupported save modes

**Cause:** `Append` and `Ignore` save modes are not supported for CSV, JSON, Parquet, Text, and XML in SCOS.

**Fix:** Use `Overwrite` or `ErrorIfExists` save modes:
```scala
// BEFORE (not supported):
df.write.mode("append").csv("@STAGE/output")

// AFTER:
df.write.mode("overwrite").csv("@STAGE/output")
```

### Unsupported file formats

**Cause:** Avro and ORC file formats are not supported in SCOS.

**Fix:** Convert to Parquet format. Note: downstream consumers expecting the original format must be updated too.
```scala
// BEFORE:
val df = spark.read.format("avro").load("data.avro")

// AFTER:
val df = spark.read.parquet("data.parquet")
```

---

## Cross-File Consistency Issues

### Error: method/parameter not found after migration

**Cause:** A method signature was changed in one file (e.g., removing `hdfs: FileSystem` parameter) but callers in other files were not updated. This is the most common cause of compilation failures after migration.

**Fix:** After every signature change, grep the entire codebase for callers:
```bash
grep -rn "methodName" <MIGRATED>/ --include="*.scala"
```
Update every call site to match the new signature.

---

### Error: Catalyst class not found (QualifiedTableName, TableIdentifier, etc.)

**Cause:** `org.apache.spark.sql.catalyst.*` classes are Spark internals not exposed via Spark Connect.

**Fix:** Define a local replacement case class with the same interface:
```scala
case class QualifiedTableName(database: String, name: String) {
  override def toString: String = s"$database.$name"
}
```

---

### Error: Hadoop / FileSystem class not found

**Cause:** `org.apache.hadoop.*` classes are not available in SCOS. Code using `FileSystem`, `Path`, `hadoopConfiguration` will not compile.

**Fix:** Remove all Hadoop imports and usages. Replace HDFS operations with Snowflake stage operations or DataFrame I/O.

---

### Error: Hive Warehouse Connector not found

**Cause:** `com.hortonworks.spark.sql.hive.*` or `enableHiveSupport()` is not available in SCOS.

**Fix:** Remove all Hive integration code. Hive tables must be migrated to Snowflake tables separately.

---

### Error: pom.xml / build.sbt version incompatibility

**Cause:** The build file still declares Scala 2.11, Spark 2.x, or Java 8 targets.

**Fix:** Update to Scala 2.12+, Spark 3.5+, Java 11+. Replace `spark-core`/`spark-sql` with `spark-connect-client-jvm`. Remove Hive, Hadoop, and incompatible library dependencies.

---

### Error: Tests fail with "Connection refused" on sc://localhost:15002

**Cause:** Test files were converted to use Spark Connect remote URL, but no server is running.

**Fix:** Test files should keep `master("local[*]")` for local/CI execution. Only production entrypoints should use `.remote("sc://localhost:15002")`.
