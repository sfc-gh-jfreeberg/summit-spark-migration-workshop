# RDD to DataFrame Conversion Reference — Scala

RDD operations are **not supported** in SCOS. Convert them to DataFrame equivalents using native functions over UDFs when possible.

## Quick Reference Table

| RDD Operation | DataFrame Equivalent |
|---------------|---------------------|
| `sc.parallelize(data)` | `spark.createDataFrame(data.toList.asJava, schema)` or `data.toDF(...)` |
| `sc.textFile(path)` | `spark.read.text(path)` |
| `df.rdd` | Remove — use DataFrame directly |
| `rdd.map(f)` | `df.select(...)` or `df.withColumn(...)` |
| `rdd.flatMap(f)` | `df.select(explode(...))` |
| `rdd.filter(f)` | `df.filter(condition)` |
| `rdd.mapValues(f)` | `df.withColumn("value", expr)` |
| `rdd.reduce(f)` | `df.agg(sum/max/min(...))` |
| `rdd.reduceByKey(f)` | `df.groupBy("key").agg(...)` |
| `rdd.groupByKey()` | `df.groupBy("key").agg(collect_list(...))` |
| `rdd.sortByKey()` | `df.orderBy("key")` |
| `rdd.join(other)` | `df.join(other_df, "key")` |
| `rdd.union/distinct/collect/count` | Direct DataFrame equivalents |
| `rdd.repartition/coalesce` | Remove (no-op in SCOS) |

## Key Examples

**Word Count (flatMap + reduceByKey):**
```scala
// BEFORE: sc.textFile("data.txt").flatMap(_.split(" ")).map(w => (w, 1)).reduceByKey(_ + _)
// AFTER:
import org.apache.spark.sql.functions.{explode, split, col, count}

spark.read.text("data.txt")
  .select(explode(split(col("value"), " ")).alias("word"))
  .groupBy("word")
  .agg(count("*").alias("count"))
```

**GroupByKey:**
```scala
// BEFORE: rdd.groupByKey()
// AFTER:
import org.apache.spark.sql.functions.collect_list

df.groupBy("key").agg(collect_list("value").alias("values"))
```

**Map with arithmetic:**
```scala
// BEFORE: rdd.map(x => x * 2)
// AFTER:
df.select((col("value") * 2).alias("value"))
```

**Filter:**
```scala
// BEFORE: rdd.filter(_ > 10)
// AFTER:
df.filter(col("value") > 10)
```

**Join:**
```scala
// BEFORE: rdd1.join(rdd2)
// AFTER:
df1.join(df2, "key")  // or df1.join(df2, Seq("key"), "left") for leftOuterJoin
```

**Parallelize (SparkContext):**
```scala
// BEFORE:
val sc = new SparkContext(conf)
val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5))
val result = rdd.map(_ * 2).collect()

// AFTER:
import spark.implicits._
val df = Seq(1, 2, 3, 4, 5).toDF("value")
val result = df.select((col("value") * 2).alias("value"))
result.show()
```

**UDF Fallback** (only when native functions won't work):
```scala
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType

val complexTransform = udf((value: String) => value.toUpperCase + "_processed")

df.select(complexTransform(col("name")).alias("result"))
```

## Scala-Specific Considerations

- Use `spark.implicits._` for implicit conversions from Scala collections to DataFrames
- Prefer `$"columnName"` syntax (requires `spark.implicits._`) or `col("columnName")`
- When converting `sc.parallelize`, use `Seq(...).toDF(...)` with the implicit conversions
- For typed transformations, prefer `Dataset` API with case classes over RDD `.map()`
