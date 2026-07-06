# Lab 4B - Spark SQL

## Objective

Use Spark SQL to query structured event data with temporary views, SQL statements, joins, set operations, partitioned writes, and caching.

## Scenario

You are analyzing StreamFlow app events for product and operations teams.
The raw events are stored as JSON Lines files.
Your job is to load the data into Spark, register SQL views, and answer common analytics questions using SQL.

This lab focuses on the Spark SQL interface.
You will still use PySpark to start Spark and load data, but most transformations should happen through `spark.sql()`.

## What You Will Build

You will create:

* A JSON Lines event dataset.
* A small user lookup dataset.
* A Spark SQL analysis script.
* SQL queries for selecting, filtering, aggregating, joining, set operations, and sorting.
* Partitioned output written by Spark.

## Prerequisites

* Python 3.10 or later.
* Java 11 or later.
* PySpark installed locally.
* Lab 4 completed or equivalent DataFrame familiarity.

> Windows note: local Parquet writes can fail on Windows if Spark cannot find Hadoop's `winutils.exe` helper. If you see `HADOOP_HOME and hadoop.home.dir are unset` or `Cannot run program ... winutils.exe`, install a compatible `winutils.exe` and set the Hadoop environment variables before running the script.
>
> ```bash
> mkdir -p /c/hadoop/bin
> curl -L https://github.com/steveloughran/winutils/raw/master/hadoop-3.3.1/bin/winutils.exe -o /c/hadoop/bin/winutils.exe
> export HADOOP_HOME=/c/hadoop
> export PATH="$HADOOP_HOME/bin:$PATH"
> export hadoop.home.dir="$HADOOP_HOME"
> ```
>
> If you are using PowerShell or Command Prompt, set the same values as environment variables instead of using `export`.

## Suggested Folder

From your lab workspace:

```bash
mkdir -p lab-04b-spark-sql/data
cd lab-04b-spark-sql
touch data/events.jsonl data/users.jsonl spark_sql_analysis.py
```

Install PySpark in a virtual environment if needed:

```bash
python -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
pip install pyspark
```

Check that PySpark imports:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

## Input Data

Create `data/events.jsonl`:

```json
{"event_id":"evt_001","event_type":"page_view","user_id":"user_101","event_ts":"2026-07-06T14:00:00Z","source":"web","session_id":"sess_001","amount":null}
{"event_id":"evt_002","event_type":"video_play","user_id":"user_102","event_ts":"2026-07-06T14:03:00Z","source":"mobile","session_id":"sess_002","amount":null}
{"event_id":"evt_003","event_type":"purchase","user_id":"user_103","event_ts":"2026-07-06T14:07:00Z","source":"web","session_id":"sess_003","amount":19.99}
{"event_id":"evt_004","event_type":"page_view","user_id":"user_101","event_ts":"2026-07-06T14:09:00Z","source":"web","session_id":"sess_001","amount":null}
{"event_id":"evt_005","event_type":"add_to_cart","user_id":"user_104","event_ts":"2026-07-06T14:12:00Z","source":"mobile","session_id":"sess_004","amount":null}
{"event_id":"evt_006","event_type":"purchase","user_id":"user_104","event_ts":"2026-07-06T14:18:00Z","source":"mobile","session_id":"sess_004","amount":9.99}
{"event_id":"evt_007","event_type":"page_view","user_id":"user_105","event_ts":"2026-07-07T09:01:00Z","source":"web","session_id":"sess_005","amount":null}
{"event_id":"evt_008","event_type":"purchase","user_id":"user_106","event_ts":"2026-07-07T09:05:00Z","source":"web","session_id":"sess_006","amount":29.99}
{"event_id":"evt_009","event_type":"video_play","user_id":"user_102","event_ts":"2026-07-07T09:08:00Z","source":"mobile","session_id":"sess_007","amount":null}
{"event_id":"evt_010","event_type":"purchase","user_id":"user_999","event_ts":"2026-07-07T09:15:00Z","source":"partner","session_id":"sess_008","amount":49.99}
```

Create `data/users.jsonl`:

```json
{"user_id":"user_101","plan":"free","region":"us-east"}
{"user_id":"user_102","plan":"premium","region":"us-west"}
{"user_id":"user_103","plan":"premium","region":"us-east"}
{"user_id":"user_104","plan":"free","region":"us-central"}
{"user_id":"user_105","plan":"free","region":"us-east"}
{"user_id":"user_106","plan":"premium","region":"us-west"}
```

Notice that `user_999` appears in the events file but not in the users file.
That row is useful for join practice.

## Spark SQL Script

Create `spark_sql_analysis.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark():
    return (
        SparkSession.builder
        .appName("Lab04BSparkSQL")
        .master("local[*]")
        .getOrCreate()
    )


def show_section(title, df):
    print(f"\n=== {title} ===")
    df.show(truncate=False)


def main():
    spark = build_spark()

    event_schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), True),
            T.StructField("event_type", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("event_ts", T.StringType(), True),
            T.StructField("source", T.StringType(), True),
            T.StructField("session_id", T.StringType(), True),
            T.StructField("amount", T.DoubleType(), True),
        ]
    )

    user_schema = T.StructType(
        [
            T.StructField("user_id", T.StringType(), True),
            T.StructField("plan", T.StringType(), True),
            T.StructField("region", T.StringType(), True),
        ]
    )

    events_df = (
        spark.read
        .schema(event_schema)
        .json("data/events.jsonl")
        .withColumn("event_time", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_time"))
    )

    users_df = spark.read.schema(user_schema).json("data/users.jsonl")

    events_df.createOrReplaceTempView("events")
    users_df.createOrReplaceTempView("users")

    show_section(
        "1. Select and filter web events",
        spark.sql("""
            SELECT event_id, event_type, user_id, event_time, source
            FROM events
            WHERE source = 'web'
            ORDER BY event_time
        """),
    )

    show_section(
        "2. Aggregate events by type and source",
        spark.sql("""
            SELECT
                event_type,
                source,
                COUNT(*) AS event_count,
                COUNT(DISTINCT user_id) AS unique_users,
                MIN(event_time) AS first_event_time,
                MAX(event_time) AS last_event_time
            FROM events
            GROUP BY event_type, source
            ORDER BY event_type, source
        """),
    )

    show_section(
        "3. Purchase revenue by plan and region",
        spark.sql("""
            SELECT
                COALESCE(u.plan, 'unknown') AS plan,
                COALESCE(u.region, 'unknown') AS region,
                COUNT(*) AS purchase_count,
                ROUND(SUM(e.amount), 2) AS revenue
            FROM events e
            LEFT JOIN users u
                ON e.user_id = u.user_id
            WHERE e.event_type = 'purchase'
            GROUP BY COALESCE(u.plan, 'unknown'), COALESCE(u.region, 'unknown')
            ORDER BY revenue DESC
        """),
    )

    show_section(
        "4. Events with missing user lookup records",
        spark.sql("""
            SELECT e.event_id, e.user_id, e.event_type, e.source
            FROM events e
            LEFT ANTI JOIN users u
                ON e.user_id = u.user_id
            ORDER BY e.event_id
        """),
    )

    show_section(
        "5. Set operation: users who purchased but are not premium",
        spark.sql("""
            SELECT user_id
            FROM events
            WHERE event_type = 'purchase'

            EXCEPT

            SELECT user_id
            FROM users
            WHERE plan = 'premium'
        """),
    )

    enriched_df = spark.sql("""
        SELECT
            e.event_id,
            e.event_type,
            e.user_id,
            e.source,
            e.event_time,
            e.event_date,
            e.amount,
            COALESCE(u.plan, 'unknown') AS plan,
            COALESCE(u.region, 'unknown') AS region
        FROM events e
        LEFT JOIN users u
            ON e.user_id = u.user_id
    """)

    enriched_df.createOrReplaceTempView("enriched_events")
    enriched_df.cache()
    print("\nCached enriched_events row count:", enriched_df.count())

    show_section(
        "6. Query the cached enriched view",
        spark.sql("""
            SELECT event_date, plan, COUNT(*) AS event_count
            FROM enriched_events
            GROUP BY event_date, plan
            ORDER BY event_date, plan
        """),
    )

    (
        enriched_df
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet("output/enriched_events_by_date")
    )

    enriched_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
```

## Run the Script

```bash
python spark_sql_analysis.py
```

Inspect the partitioned output:

```bash
ls output/enriched_events_by_date
```

Spark should create folders similar to:

```text
event_date=2026-07-06/
event_date=2026-07-07/
```

## Checkpoints

You are done when:

* The script creates a `SparkSession`.
* The script registers `events` and `users` temporary views.
* SQL queries run with `spark.sql()`.
* The output includes filtered rows, grouped summaries, join results, and a set operation result.
* The enriched output is written as partitioned Parquet files.
* You can explain why `user_999` appears in the missing lookup query.

## Practice Questions

Answer these in your lab notes:

1. Why do we create temporary views before calling `spark.sql()`?
2. What is the difference between a DataFrame transformation and a SQL query in Spark?
3. Why does the revenue query use a `LEFT JOIN` instead of an `INNER JOIN`?
4. What does the `LEFT ANTI JOIN` return?
5. Why might partitioning by `event_date` help later queries?
6. When is caching useful, and why should we avoid caching every DataFrame?

## Stretch Tasks

Try these after the main lab works:

* Rewrite query 2 using the DataFrame API and compare the output.
* Add a `CASE WHEN` expression that labels purchases as `small`, `medium`, or `large`.
* Add a query that finds the top source by event count for each date.
* Write the aggregate summary to `output/event_summary_sql` as CSV.
* Replace one temporary view with a global temporary view and query it using `global_temp`.

## Deliverables

Submit:

* `data/events.jsonl`.
* `data/users.jsonl`.
* `spark_sql_analysis.py`.
* A screenshot or copied output from the revenue by plan and region query.
* A screenshot or copied output showing the partitioned output folders.
* Short answers to the practice questions.

## Common Issues

| Problem | Likely Cause | Fix |
| ------- | ------------ | --- |
| `TABLE_OR_VIEW_NOT_FOUND` | Temporary view was not registered | Confirm `createOrReplaceTempView()` ran before `spark.sql()` |
| SQL syntax error | Missing comma, quote, or alias | Run one query at a time and check line numbers |
| Timestamp values are null | Spark could not parse `event_ts` | Compare the input timestamp format to the sample data |
| Output path already exists | Spark will not overwrite unless configured | This script uses `mode("overwrite")`; confirm the path is correct |
| Empty join result | Join keys do not match | Compare `user_id` values in both input files |
| Parquet write fails on Windows | Missing Hadoop `winutils.exe` or unset Hadoop environment variables | Install a compatible `winutils.exe` and set `HADOOP_HOME`, `hadoop.home.dir`, and `PATH` before running the script |

## Cleanup

When finished:

```bash
deactivate
```