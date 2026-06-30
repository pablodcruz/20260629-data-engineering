# Lab 4 - PySpark DataFrames

## Objective

Use PySpark DataFrames to load, clean, transform, summarize, and write structured event data.

## Scenario

You have received raw event data from the StreamFlow app.
Before this data can be used in a pipeline, you need to inspect the schema, remove invalid records, and create a small summary table.

## What You Will Build

You will create:

* A JSON Lines input dataset.
* A PySpark transformation script.
* A cleaned event DataFrame.
* A summary output grouped by `event_type` and `source`.

## Prerequisites

* Python 3.10 or later.
* Java 11 or later.
* PySpark installed locally.

## Suggested Folder

From your lab workspace:

```bash
mkdir -p lab-04-pyspark/data
cd lab-04-pyspark
touch data/events.jsonl transform_events.py
```

Install PySpark in a virtual environment:

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
{"event_id":"evt_001","event_type":"page_view","user_id":"user_101","event_ts":"2026-06-30T14:00:00Z","source":"web","payload":{"page":"/home"}}
{"event_id":"evt_002","event_type":"video_play","user_id":"user_102","event_ts":"2026-06-30T14:01:00Z","source":"mobile","payload":{"video_id":"vid_001"}}
{"event_id":"evt_003","event_type":"purchase","user_id":"user_103","event_ts":"2026-06-30T14:03:00Z","source":"web","payload":{"amount":"19.99"}}
{"event_id":"evt_004","event_type":"page_view","user_id":"user_101","event_ts":"2026-06-30T14:05:00Z","source":"web","payload":{"page":"/pricing"}}
{"event_id":"evt_005","event_type":"add_to_cart","user_id":"user_104","event_ts":"2026-06-30T14:06:00Z","source":"mobile","payload":{"sku":"sku_001"}}
{"event_id":"","event_type":"purchase","user_id":"user_105","event_ts":"2026-06-30T14:07:00Z","source":"web","payload":{"amount":"10.00"}}
{"event_id":"evt_007","event_type":"page_view","user_id":"","event_ts":"not-a-timestamp","source":"web","payload":{"page":"/bad"}}
```

The last two rows are intentionally invalid.

## Transformation Script

Create `transform_events.py`:

```python
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark():
    return (
        SparkSession.builder
        .appName("Lab04PySparkDataFrames")
        .master("local[*]")
        .getOrCreate()
    )


def main(input_path, output_path):
    spark = build_spark()

    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), True),
            T.StructField("event_type", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("event_ts", T.StringType(), True),
            T.StructField("source", T.StringType(), True),
            T.StructField("payload", T.MapType(T.StringType(), T.StringType()), True),
        ]
    )

    raw_df = spark.read.schema(schema).json(input_path)

    print("Raw schema:")
    raw_df.printSchema()

    prepared_df = (
        raw_df
        .withColumn("event_time", F.to_timestamp("event_ts"))
        .withColumn("ingest_date", F.to_date("event_time"))
    )

    valid_df = prepared_df.filter(
        (F.col("event_id").isNotNull())
        & (F.length(F.col("event_id")) > 0)
        & (F.col("user_id").isNotNull())
        & (F.length(F.col("user_id")) > 0)
        & (F.col("event_time").isNotNull())
    )

    invalid_df = prepared_df.exceptAll(valid_df)

    print("Valid records:")
    valid_df.select("event_id", "event_type", "user_id", "event_time", "source").show(truncate=False)

    print("Invalid records:")
    invalid_df.select("event_id", "event_type", "user_id", "event_ts", "source").show(truncate=False)

    summary_df = (
        valid_df
        .groupBy("event_type", "source")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.min("event_time").alias("first_event_time"),
            F.max("event_time").alias("last_event_time"),
        )
        .orderBy("event_type", "source")
    )

    print("Summary:")
    summary_df.show(truncate=False)

    (
        summary_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    input_arg = sys.argv[1] if len(sys.argv) > 1 else "data/events.jsonl"
    output_arg = sys.argv[2] if len(sys.argv) > 2 else "output/event_summary"
    main(input_arg, output_arg)
```

## Run the Script

```bash
python transform_events.py data/events.jsonl output/event_summary
```

Inspect the output folder:

```bash
ls output/event_summary
```

Spark writes a folder, not a single file.
Inside the folder, look for a file that starts with `part-`.

## Checkpoints

You are done when:

* The script prints the raw schema.
* Valid and invalid records are shown separately.
* The summary output includes counts by `event_type` and `source`.
* A CSV output folder is created under `output/event_summary`.

## Deliverables

Submit:

* `data/events.jsonl`.
* `transform_events.py`.
* The command used to run the script.
* A screenshot or copied output from the `Summary` table.
* A short note explaining why Spark writes output as a folder.

## Common Issues

| Problem | Likely Cause | Fix |
| ------- | ------------ | --- |
| `JAVA_HOME` error | Java is missing or not configured | Install Java 11+ and reopen the terminal |
| `ModuleNotFoundError: pyspark` | Virtual environment is not active | Run `source .venv/Scripts/activate` and reinstall PySpark |
| Output path already exists | Spark will not overwrite unless configured | This script uses `mode("overwrite")`; confirm the output path is correct |
| Empty summary | All rows failed validation | Check `event_id`, `user_id`, and timestamp values |

## Cleanup

When finished:

```bash
deactivate
```
