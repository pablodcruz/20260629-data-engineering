# Week 7 - Spark SQL / DataFrames / Datasets

## Learning Outcomes

By the end of this week you will be able to:

* Explain the purpose of **Spark SQL** in the Spark ecosystem.
* Create and use a **SparkSession** as the main Spark entry point.
* Compare **SparkSession**, **SparkContext**, `SQLContext`, and `HiveContext`.
* Create, inspect, and transform **DataFrames**.
* Select, filter, sort, aggregate, and join structured data.
* Work with JSON data using Spark DataFrame APIs.
* Understand the role of **Datasets** and how they relate to DataFrames.
* Use SQL queries against temporary views.
* Add, remove, and rename columns.
* Apply set operations such as union, intersect, and except.
* Explain partitioning, bucketing, and caching at a practical level.

---

## Spark SQL Overview

### Definition

**Spark SQL** is Spark's module for working with structured and semi-structured data.
It lets you process data using SQL queries, DataFrames, and Datasets.

Spark SQL sits above Spark Core and uses Spark's distributed execution engine.

### Why Spark SQL Matters

* Provides a familiar SQL interface for data analysis.
* Optimizes queries using Spark's **Catalyst optimizer**.
* Works with structured formats like JSON, CSV, Parquet, ORC, and Hive tables.
* Supports both code-based transformations and SQL text queries.
* Makes Spark easier to use than low-level RDD APIs for tabular data.

### Spark SQL vs RDDs

| Feature | RDDs | Spark SQL / DataFrames |
| ------- | ---- | ---------------------- |
| Data structure | Distributed collection of objects | Distributed table with schema |
| Optimization | Mostly manual | Catalyst optimizer |
| API style | Functional transformations | SQL-like and DataFrame operations |
| Schema | Optional / manual | Built in |
| Best for | Low-level control | Structured data processing |

---

## SparkSession

### Definition

**SparkSession** is the main entry point for modern Spark applications.
It was introduced to unify older Spark entry points into one object.

A SparkSession gives access to:

* DataFrame creation
* Spark SQL queries
* SparkContext
* Reading and writing data
* Catalog and table metadata

### Creating a SparkSession

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Week7SparkSQL") \
    .master("local[*]") \
    .getOrCreate()
```

### Accessing SparkContext

```python
sc = spark.sparkContext
print(sc.appName)
```

### Common SparkSession Methods

| Method | Purpose |
| ------ | ------- |
| `spark.read` | Load data into DataFrames |
| `spark.sql()` | Run SQL queries |
| `spark.createDataFrame()` | Create a DataFrame from local data or RDDs |
| `spark.table()` | Read a table or view |
| `spark.catalog` | Inspect databases, tables, and views |
| `spark.stop()` | Stop the Spark application |

---

## SparkSession vs SparkContext

### SparkContext

**SparkContext** is the older low-level entry point for Spark Core.
It is mainly used for RDD operations and cluster communication.

```python
sc = spark.sparkContext
rdd = sc.parallelize([1, 2, 3])
```

### SparkSession

**SparkSession** is the higher-level entry point used for DataFrames, SQL, and structured data.

```python
df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)
```

| Object | Main Purpose | Typical Use |
| ------ | ------------ | ----------- |
| **SparkContext** | Low-level Spark Core access | RDDs |
| **SparkSession** | Unified structured data entry point | DataFrames, SQL, Datasets |

### Key Idea

For modern PySpark work, start with `SparkSession`.
Use `spark.sparkContext` only when you specifically need RDD functionality.

---

## SQLContext and HiveContext

### SQLContext

`SQLContext` was the older entry point for Spark SQL before SparkSession.
It provided SQL and DataFrame functionality.

### HiveContext

`HiveContext` extended `SQLContext` with Hive support, including HiveQL and access to Hive metastore tables.

### Modern Replacement

In modern Spark, **SparkSession replaces both**.

```python
spark = SparkSession.builder \
    .appName("HiveEnabledApp") \
    .enableHiveSupport() \
    .getOrCreate()
```

| Older Object | Modern Replacement |
| ------------ | ------------------ |
| `SparkContext` | Still available for RDDs |
| `SQLContext` | `SparkSession` |
| `HiveContext` | `SparkSession.builder.enableHiveSupport()` |

---

## Introduction to DataFrames

### Definition

A **DataFrame** is a distributed table with rows and named columns.
It is similar to a table in SQL or a DataFrame in pandas, but it can scale across a Spark cluster.

### DataFrame Characteristics

* Has a schema.
* Is distributed across partitions.
* Uses lazy evaluation.
* Supports SQL-style operations.
* Can be optimized by Spark.

### Creating a DataFrame from Python Data

```python
data = [
    ("Ada", "Engineering", 95),
    ("Grace", "Analytics", 98),
    ("Alan", "Engineering", 88)
]

columns = ["name", "department", "score"]

df = spark.createDataFrame(data, columns)
df.show()
```

### Inspecting DataFrames

```python
df.show()
df.printSchema()
df.columns
df.count()
df.describe().show()
```

### Example Schema Output

```text
root
 |-- name: string (nullable = true)
 |-- department: string (nullable = true)
 |-- score: long (nullable = true)
```

---

## Reading and Writing DataFrames

### Reading CSV

```python
df = spark.read.csv(
    "data/students.csv",
    header=True,
    inferSchema=True
)
```

### Reading JSON

```python
json_df = spark.read.json("data/events.json")
```

### Reading Parquet

```python
parquet_df = spark.read.parquet("data/sales.parquet")
```

### Writing Data

```python
df.write.mode("overwrite").csv("output/students_csv", header=True)
df.write.mode("overwrite").parquet("output/students_parquet")
df.write.mode("overwrite").json("output/students_json")
```

### Common Write Modes

| Mode | Behavior |
| ---- | -------- |
| `error` | Fails if output already exists |
| `overwrite` | Replaces existing output |
| `append` | Adds new files to existing output |
| `ignore` | Does nothing if output exists |

---

## DataFrame Operations

### Selecting Columns

```python
df.select("name", "score").show()
```

```python
from pyspark.sql.functions import col

df.select(col("name"), col("score")).show()
```

### Aliasing Columns

```python
df.select(
    col("name"),
    col("score").alias("final_score")
).show()
```

### Filtering Rows

```python
df.filter(col("score") >= 90).show()
```

```python
df.where("score >= 90").show()
```

### Multiple Conditions

```python
df.filter(
    (col("department") == "Engineering") & (col("score") >= 90)
).show()
```

### Sorting

```python
df.orderBy("score").show()
df.orderBy(col("score").desc()).show()
```

---

## Adding, Removing, and Renaming Columns

### Adding Columns

Use `withColumn()` to add or replace a column.

```python
from pyspark.sql.functions import lit

df_with_status = df.withColumn("passed", col("score") >= 70)
df_with_school = df.withColumn("school", lit("Data Foundation"))
```

### Transforming Existing Columns

```python
df_scaled = df.withColumn("score_scaled", col("score") / 100)
```

### Renaming Columns

```python
df_renamed = df.withColumnRenamed("score", "exam_score")
```

### Removing Columns

```python
df_dropped = df.drop("department")
```

### Important Note

DataFrames are immutable.
Each operation returns a new DataFrame instead of changing the original one in place.

---

## Aggregate Functions

### Definition

Aggregate functions summarize multiple rows into a smaller result.

Common aggregate functions:

| Function | Purpose |
| -------- | ------- |
| `count()` | Count rows |
| `sum()` | Add values |
| `avg()` / `mean()` | Calculate average |
| `min()` | Minimum value |
| `max()` | Maximum value |

### Basic Aggregation

```python
from pyspark.sql.functions import avg, count, max, min

df.select(
    count("*").alias("row_count"),
    avg("score").alias("avg_score"),
    min("score").alias("min_score"),
    max("score").alias("max_score")
).show()
```

### Grouped Aggregation

```python
df.groupBy("department") \
  .agg(
      count("*").alias("student_count"),
      avg("score").alias("avg_score")
  ) \
  .show()
```

### Having-Style Filter

```python
summary = df.groupBy("department") \
    .agg(avg("score").alias("avg_score"))

summary.filter(col("avg_score") >= 90).show()
```

---

## Joins

### Definition

A **join** combines rows from two DataFrames based on matching column values.

### Example DataFrames

```python
students = spark.createDataFrame(
    [(1, "Ada"), (2, "Grace"), (3, "Alan")],
    ["student_id", "name"]
)

scores = spark.createDataFrame(
    [(1, 95), (2, 98), (4, 80)],
    ["student_id", "score"]
)
```

### Inner Join

```python
students.join(scores, on="student_id", how="inner").show()
```

### Common Join Types

| Join Type | Meaning |
| --------- | ------- |
| `inner` | Keep matching rows only |
| `left` / `left_outer` | Keep all left rows and matching right rows |
| `right` / `right_outer` | Keep all right rows and matching left rows |
| `outer` / `full_outer` | Keep rows from both sides |
| `left_semi` | Keep left rows that have a match |
| `left_anti` | Keep left rows that do not have a match |

### Join with Different Column Names

```python
students.join(
    scores,
    students.student_id == scores.student_id,
    "inner"
).show()
```

### Join Performance Tip

If one DataFrame is small, Spark may use a broadcast join.

```python
from pyspark.sql.functions import broadcast

result = large_df.join(
    broadcast(small_lookup_df),
    on="lookup_id",
    how="left"
)
```

---

## Working with JSON Datasets

### Definition

JSON is a common semi-structured data format used in APIs, logs, event streams, and configuration files.
Spark can infer schemas from JSON and load records into DataFrames.

### Reading JSON

```python
events = spark.read.json("data/events.json")
events.printSchema()
events.show(truncate=False)
```

### Selecting Nested Fields

```python
events.select(
    col("event_id"),
    col("user.id").alias("user_id"),
    col("user.country").alias("country")
).show()
```

### Exploding Arrays

```python
from pyspark.sql.functions import explode

events.select(
    col("event_id"),
    explode(col("items")).alias("item")
).show()
```

### JSON Tips

* Use `printSchema()` before writing transformations.
* Nested fields can be selected with dot notation.
* Arrays often need `explode()`.
* Explicit schemas are better than inference for production pipelines.

---

## Introduction to Datasets

### Definition

A **Dataset** is Spark's strongly typed distributed collection API.
It combines some benefits of RDDs and DataFrames.

Datasets are mainly used in Scala and Java.
In PySpark, DataFrames are the primary structured API because Python does not provide the same compile-time type safety.

### DataFrame vs Dataset

| Feature | DataFrame | Dataset |
| ------- | --------- | ------- |
| Main languages | Python, Scala, Java, R | Scala, Java |
| Type safety | Runtime schema checks | Compile-time type checks |
| Optimization | Catalyst optimizer | Catalyst optimizer |
| Python support | Primary API | Not directly typed like Scala/Java |

### Key Idea for PySpark

In PySpark, focus on DataFrames.
When course material mentions Datasets, think of them as typed DataFrames used mostly in Scala and Java Spark applications.

---

## Spark SQL Queries

### Temporary Views

To query a DataFrame using SQL, register it as a temporary view.

```python
df.createOrReplaceTempView("students")
```

### Running SQL

```python
spark.sql("""
    SELECT department, AVG(score) AS avg_score
    FROM students
    GROUP BY department
    ORDER BY avg_score DESC
""").show()
```

### SQL and DataFrame APIs Are Connected

These two examples express the same idea:

```python
df.filter(col("score") >= 90).select("name", "score").show()
```

```python
spark.sql("""
    SELECT name, score
    FROM students
    WHERE score >= 90
""").show()
```

### Temporary vs Global Temporary Views

| View Type | Scope |
| --------- | ----- |
| Temporary view | Current SparkSession |
| Global temporary view | Shared across SparkSessions in the same application |

```python
df.createGlobalTempView("students_global")
spark.sql("SELECT * FROM global_temp.students_global").show()
```

---

## Set Operations

### Definition

Set operations combine or compare two DataFrames, similar to set theory in mathematics.
They are useful when you need to merge datasets, find common rows, or identify rows that exist in one dataset but not another.

Common use cases:

| Operation | Question It Answers |
| --------- | ------------------- |
| `union()` | How do I combine rows from both DataFrames? |
| `intersect()` | Which rows appear in both DataFrames? |
| `exceptAll()` / `subtract()` | Which rows are in the first DataFrame but not the second? |

### Schema Requirement

A critical requirement is that both DataFrames must have:

* The same number of columns.
* Compatible data types in matching positions.

For most set operations, Spark compares columns by position, not by name.
This means column order matters.
Use `unionByName()` when column names should control alignment.

### Example DataFrames

```python
spring = spark.createDataFrame(
    [(1, "Ada"), (2, "Grace")],
    ["id", "name"]
)

summer = spark.createDataFrame(
    [(2, "Grace"), (3, "Alan")],
    ["id", "name"]
)
```

### Union

`union()` combines rows from two DataFrames.
In PySpark, it keeps duplicates.

```python
spring.union(summer).show()
```

Use `distinct()` if you want unique rows.

```python
spring.union(summer).distinct().show()
```

Expected idea:

```text
union: Ada, Grace, Grace, Alan
union + distinct: Ada, Grace, Alan
```

### Intersect

`intersect()` returns rows that appear in both DataFrames.
It behaves like an `INTERSECT DISTINCT`, so duplicate common rows are not repeated.

```python
spring.intersect(summer).show()
```

Expected idea:

```text
Grace
```

### Except

`exceptAll()` returns rows from the first DataFrame that are not removed by matching rows in the second DataFrame.
It is duplicate-aware.

```python
spring.exceptAll(summer).show()
```

For a simpler "left side minus right side" explanation, you may also see `subtract()`:

```python
spring.subtract(summer).show()
```

Expected idea:

```text
Ada
```

### Set Operation Notes

* Schemas must have the same number of columns and compatible data types.
* Column order matters for position-based operations.
* `unionByName()` is safer when column names should control alignment.
* `union()` keeps duplicates; add `distinct()` when you need unique rows.
* These operations can trigger shuffles because Spark may need to compare rows across partitions.

```python
combined = spring.unionByName(summer)
```

"Set operations in PySpark are used to combine or compare DataFrames. For example, `union()` stacks rows, `intersect()` finds rows in both DataFrames, and `exceptAll()` or `subtract()` finds rows from one DataFrame that are not in another. The main requirement is that the DataFrames have the same number of columns with compatible types, and column order matters unless we use `unionByName()`."

---

## Sorting and Partitioning

### Sorting

Sorting controls row order in output or query results.

```python
df.orderBy(col("department"), col("score").desc()).show()
```

### Partitioning

Partitioning controls how data is distributed across Spark tasks or written to storage.

```python
repartitioned = df.repartition(8)
```

### Repartition vs Coalesce

| Operation | Purpose | Shuffle? |
| --------- | ------- | -------- |
| `repartition(n)` | Increase or rebalance partitions | Yes |
| `coalesce(n)` | Reduce partitions | Usually avoids full shuffle |

### Partitioned Writes

Partitioned writes organize output folders by column values.

```python
df.write.mode("overwrite") \
  .partitionBy("department") \
  .parquet("output/students_by_department")
```

Example output:

```text
students_by_department/
  department=Analytics/
  department=Engineering/
```

### When Partitioning Helps

Partitioning helps when queries often filter by the partition column.

Example:

```python
spark.read.parquet("output/students_by_department") \
    .filter(col("department") == "Engineering") \
    .show()
```

Spark can skip irrelevant partition folders.

---

## Bucketing

### Definition

**Bucketing** divides data into a fixed number of files based on a hash of one or more columns.
It is often used to optimize joins and repeated queries on large tables.

### Partitioning vs Bucketing

| Feature | Partitioning | Bucketing |
| ------- | ------------ | --------- |
| Organizes by | Folder values | Hash buckets |
| Best for | Filtering by column | Joins and grouped operations |
| Output layout | One folder per value | Fixed number of bucket files |
| Risk | Too many small folders | Requires table-based workflow |

### Bucketed Table Example

```python
df.write \
  .bucketBy(8, "student_id") \
  .sortBy("student_id") \
  .mode("overwrite") \
  .saveAsTable("bucketed_students")
```

### Practical Note

Bucketing is usually more advanced than simple partitioned file output.
It is most useful when Spark is reading from managed tables and repeatedly joining or aggregating by the same keys.

---

## Spark Caching Overview

### Definition

**Caching** stores a DataFrame, Dataset, or RDD in memory or disk so Spark can reuse it without recomputing the full lineage.

### When to Cache

Cache when:

* The same DataFrame is reused multiple times.
* A transformation chain is expensive.
* You are doing iterative analysis.
* Multiple actions are run against the same intermediate result.

### Basic Cache Example

```python
clean = df.filter(col("score").isNotNull()).cache()

clean.count()
clean.groupBy("department").avg("score").show()
clean.orderBy(col("score").desc()).show()
```

The first action materializes the cache.
Later actions can reuse it.

### Cache vs Persist

| Method | Meaning |
| ------ | ------- |
| `cache()` | Stores data using Spark's default storage level |
| `persist()` | Lets you choose a storage level |
| `unpersist()` | Removes cached data |

```python
clean.unpersist()
```

### Caching Warnings

* Caching uses executor memory.
* Do not cache every DataFrame.
* Cache only reused data.
* Always unpersist large cached data when it is no longer needed.

---

## End-to-End DataFrame Example

### Student Score Pipeline

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count

spark = SparkSession.builder \
    .appName("StudentScorePipeline") \
    .master("local[*]") \
    .getOrCreate()

students = spark.read.csv(
    "data/students.csv",
    header=True,
    inferSchema=True
)

clean = students.filter(col("score").isNotNull()) \
    .withColumn("passed", col("score") >= 70)

summary = clean.groupBy("department") \
    .agg(
        count("*").alias("student_count"),
        avg("score").alias("avg_score")
    ) \
    .orderBy(col("avg_score").desc())

summary.show()

summary.write.mode("overwrite") \
    .parquet("output/department_summary")

spark.stop()
```

---

## Common Mistakes and Gotchas

| Mistake | Why It Matters | Better Approach |
| ------- | -------------- | --------------- |
| Forgetting Spark is lazy | Transformations do not run until an action | Use actions like `show()`, `count()`, or writes to trigger execution |
| Using `collect()` for inspection | Pulls all data to the driver | Use `show()`, `take()`, or `limit()` |
| Depending on inferred schemas in production | Schema inference can be slow or inconsistent | Define explicit schemas |
| Using `union()` with mismatched column order | Data can be placed in the wrong columns | Use `unionByName()` |
| Caching everything | Wastes executor memory | Cache only reused DataFrames |
| Over-partitioning output | Creates too many small files | Use appropriate partition columns and file counts |
| Confusing partitioning and bucketing | They solve different problems | Partition for filtering, bucket for repeated joins/aggregations |

---

## Discussion Points

* Why is SparkSession the preferred entry point for modern Spark applications?
* What advantages do DataFrames have over RDDs for structured data?
* When would you use SQL syntax instead of DataFrame methods?
* Why can schema inference be risky in production pipelines?
* How do joins affect Spark performance?
* When should a DataFrame be cached?
* What is the difference between partitioning and bucketing?

---

## Summary

| Topic | Key Takeaway |
| ----- | ------------ |
| **Spark SQL** | Structured data layer for SQL and DataFrames |
| **SparkSession** | Main modern entry point for Spark applications |
| **DataFrame** | Distributed table with schema and optimized execution |
| **Dataset** | Strongly typed structured API used mostly in Scala/Java |
| **Selections and filters** | Core tools for narrowing columns and rows |
| **Aggregations** | Summarize data with `groupBy()` and aggregate functions |
| **Joins** | Combine DataFrames by matching keys |
| **JSON** | Common semi-structured format Spark can infer and query |
| **Set operations** | Combine or compare compatible DataFrames |
| **Partitioning** | Organizes data for filtering and parallelism |
| **Bucketing** | Organizes table data for joins and repeated key-based operations |
| **Caching** | Reuses expensive intermediate results |

Spark SQL and DataFrames are the usual next step after RDDs. They give you a higher-level, schema-aware API while still running on Spark's distributed engine. For most structured data work, DataFrames are easier to write, easier to optimize, and closer to the way data engineers query production datasets.
