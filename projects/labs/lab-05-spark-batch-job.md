# Lab 5 - Spark Batch Job

## Objective

Package PySpark transformation logic as a reusable Spark batch job that can run with `spark-submit`.

## Scenario

In production, Spark code usually runs as a submitted job instead of a one-off interactive script.
This lab turns the transformation from Lab 4 into a parameterized job that accepts input and output paths.

## What You Will Build

You will create:

* A reusable Spark job script.
* A small event input file.
* Separate outputs for summary records and rejected records.
* A repeatable `spark-submit` command.

## Prerequisites

* Lab 4 completed or equivalent PySpark setup.
* `spark-submit` available in your terminal.

Check:

```bash
spark-submit --version
```

If `spark-submit` is not recognized but PySpark is installed, try:

```bash
python -m pyspark --version
```

## Suggested Folder

From your lab workspace:

```bash
mkdir -p lab-05-spark-job/jobs lab-05-spark-job/data
cd lab-05-spark-job
touch jobs/process_events.py data/events.jsonl
```

## Input Data

Create `data/events.jsonl`:

```json
{"event_id":"evt_001","event_type":"page_view","user_id":"user_101","event_ts":"2026-06-30T14:00:00Z","source":"web","payload":{"page":"/home"}}
{"event_id":"evt_002","event_type":"video_play","user_id":"user_102","event_ts":"2026-06-30T14:01:00Z","source":"mobile","payload":{"video_id":"vid_001"}}
{"event_id":"evt_003","event_type":"purchase","user_id":"user_103","event_ts":"2026-06-30T14:03:00Z","source":"web","payload":{"amount":"19.99"}}
{"event_id":"evt_004","event_type":"purchase","user_id":"user_103","event_ts":"2026-06-30T14:04:00Z","source":"mobile","payload":{"amount":"9.99"}}
{"event_id":"evt_bad_001","event_type":"purchase","user_id":"","event_ts":"2026-06-30T14:05:00Z","source":"web","payload":{"amount":"10.00"}}
{"event_id":"evt_bad_002","event_type":"page_view","user_id":"user_105","event_ts":"not-a-timestamp","source":"web","payload":{"page":"/bad"}}
```

## Spark Job

Create `jobs/process_events.py`:

```python
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def parse_args():
    parser = argparse.ArgumentParser(description="Process StreamFlow event data.")
    parser.add_argument("--input", required=True, help="Input JSON Lines path")
    parser.add_argument("--summary-output", required=True, help="Output path for summary CSV")
    parser.add_argument("--bad-output", required=True, help="Output path for rejected records CSV")
    return parser.parse_args()


def build_spark():
    return (
        SparkSession.builder
        .appName("Lab05SparkBatchJob")
        .getOrCreate()
    )


def main():
    args = parse_args()
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

    events_df = (
        spark.read
        .schema(schema)
        .json(args.input)
        .withColumn("event_time", F.to_timestamp("event_ts"))
    )

    checked_df = (
        events_df
        .withColumn(
            "reject_reason",
            F.when(F.col("event_id").isNull() | (F.length("event_id") == 0), F.lit("missing event_id"))
            .when(F.col("user_id").isNull() | (F.length("user_id") == 0), F.lit("missing user_id"))
            .when(F.col("event_time").isNull(), F.lit("invalid event_ts"))
            .otherwise(F.lit(None)),
        )
    )

    good_df = checked_df.filter(F.col("reject_reason").isNull())
    bad_df = checked_df.filter(F.col("reject_reason").isNotNull())

    summary_df = (
        good_df
        .groupBy("event_type", "source")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .orderBy("event_type", "source")
    )

    print("Summary output:")
    summary_df.show(truncate=False)

    print("Rejected records:")
    bad_df.select("event_id", "event_type", "user_id", "event_ts", "reject_reason").show(truncate=False)

    (
        summary_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(args.summary_output)
    )

    (
        bad_df
        .select("event_id", "event_type", "user_id", "event_ts", "source", "reject_reason")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(args.bad_output)
    )

    spark.stop()


if __name__ == "__main__":
    main()
```

## Run with Spark Submit

```bash
spark-submit jobs/process_events.py \
  --input data/events.jsonl \
  --summary-output output/event_summary \
  --bad-output output/bad_events
```

The backslash character continues a Bash command on the next line.
You can also run it as one line:

```bash
spark-submit jobs/process_events.py --input data/events.jsonl --summary-output output/event_summary --bad-output output/bad_events
```

Inspect outputs:

```bash
ls output/event_summary
ls output/bad_events
```

## Checkpoints

You are done when:

* `spark-submit` runs without Python import errors.
* The terminal shows a summary table.
* The terminal shows rejected records.
* `output/event_summary` and `output/bad_events` both exist.

## Deliverables

Submit:

* `jobs/process_events.py`.
* `data/events.jsonl`.
* The `spark-submit` command you used.
* A screenshot or copied output from the summary table.
* A screenshot or copied output from the rejected records table.

## Common Issues

| Problem | Likely Cause | Fix |
| ------- | ------------ | --- |
| `spark-submit` not recognized | Spark is not installed or not on PATH | Use the PySpark environment from Lab 4 or install Spark locally |
| Script cannot find input path | Command was run from the wrong folder | Run `pwd` and confirm you are in `lab-05-spark-job` |
| Output folders contain many files | Spark writes distributed output | For the lab, `coalesce(1)` creates one data part file |
| Multi-line command fails | Bash line continuation was copied with spaces after the backslash | Run the one-line version |

## Reflection Questions

Answer briefly:

* Why is passing input and output paths as arguments better than hardcoding them?
* What would change if this job ran on a cluster instead of your laptop?
