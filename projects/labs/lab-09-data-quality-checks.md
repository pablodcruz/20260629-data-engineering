# Lab 9 - Data Quality Checks

## Objective

Add validation checks that separate good records from bad records and produce a data quality summary.

## Scenario

Pipelines should not blindly trust incoming data.
In this lab, you will check for common event data problems: missing IDs, bad timestamps, duplicate events, and invalid purchase amounts.

## What You Will Build

You will create:

* A dirty input dataset.
* A PySpark data quality job.
* A good records output.
* A bad records output with reject reasons.
* A summary table of data quality results.

## Prerequisites

* PySpark is available locally.
* Lab 4 or Lab 5 completed.

## Suggested Folder

From your lab workspace:

```bash
mkdir -p lab-09-data-quality/jobs lab-09-data-quality/data
cd lab-09-data-quality
touch data/events_dirty.jsonl jobs/run_quality_checks.py
```

## Dirty Input Data

Create `data/events_dirty.jsonl`:

```json
{"event_id":"evt_001","event_type":"page_view","user_id":"user_101","event_ts":"2026-06-30T14:00:00Z","source":"web","payload":{"page":"/home"}}
{"event_id":"evt_002","event_type":"purchase","user_id":"user_102","event_ts":"2026-06-30T14:01:00Z","source":"mobile","payload":{"amount":"19.99"}}
{"event_id":"evt_002","event_type":"purchase","user_id":"user_102","event_ts":"2026-06-30T14:02:00Z","source":"mobile","payload":{"amount":"19.99"}}
{"event_id":"","event_type":"page_view","user_id":"user_103","event_ts":"2026-06-30T14:03:00Z","source":"web","payload":{"page":"/pricing"}}
{"event_id":"evt_005","event_type":"video_play","user_id":"","event_ts":"2026-06-30T14:04:00Z","source":"mobile","payload":{"video_id":"vid_001"}}
{"event_id":"evt_006","event_type":"purchase","user_id":"user_106","event_ts":"not-a-timestamp","source":"web","payload":{"amount":"9.99"}}
{"event_id":"evt_007","event_type":"purchase","user_id":"user_107","event_ts":"2026-06-30T14:06:00Z","source":"web","payload":{"amount":"-5.00"}}
```

## Data Quality Job

Create `jobs/run_quality_checks.py`:

```python
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def parse_args():
    parser = argparse.ArgumentParser(description="Run StreamFlow data quality checks.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--good-output", required=True)
    parser.add_argument("--bad-output", required=True)
    parser.add_argument("--summary-output", required=True)
    return parser.parse_args()


def build_spark():
    return (
        SparkSession.builder
        .appName("Lab09DataQualityChecks")
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
        .withColumn("purchase_amount", F.col("payload").getItem("amount").cast("double"))
    )

    event_window = Window.partitionBy("event_id")

    checked_df = (
        events_df
        .withColumn("event_id_count", F.count("*").over(event_window))
        .withColumn(
            "reject_reason",
            F.when(F.col("event_id").isNull() | (F.length("event_id") == 0), F.lit("missing event_id"))
            .when(F.col("user_id").isNull() | (F.length("user_id") == 0), F.lit("missing user_id"))
            .when(F.col("event_time").isNull(), F.lit("invalid event_ts"))
            .when(F.col("event_id_count") > 1, F.lit("duplicate event_id"))
            .when(
                (F.col("event_type") == "purchase")
                & (F.col("purchase_amount").isNull() | (F.col("purchase_amount") < 0)),
                F.lit("invalid purchase amount"),
            )
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "dq_status",
            F.when(F.col("reject_reason").isNull(), F.lit("pass")).otherwise(F.lit("fail")),
        )
    )

    good_df = checked_df.filter(F.col("dq_status") == "pass")
    bad_df = checked_df.filter(F.col("dq_status") == "fail")

    summary_df = (
        checked_df
        .groupBy("dq_status", "reject_reason")
        .agg(F.count("*").alias("record_count"))
        .orderBy("dq_status", "reject_reason")
    )

    print("Data quality summary:")
    summary_df.show(truncate=False)

    print("Bad records:")
    bad_df.select("event_id", "event_type", "user_id", "event_ts", "reject_reason").show(truncate=False)

    (
        good_df
        .drop("event_id_count", "reject_reason", "dq_status")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(args.good_output)
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

    (
        summary_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(args.summary_output)
    )

    spark.stop()


if __name__ == "__main__":
    main()
```

## Run the Job

```bash
spark-submit jobs/run_quality_checks.py \
  --input data/events_dirty.jsonl \
  --good-output output/good_events \
  --bad-output output/bad_events \
  --summary-output output/dq_summary
```

One-line version:

```bash
spark-submit jobs/run_quality_checks.py --input data/events_dirty.jsonl --good-output output/good_events --bad-output output/bad_events --summary-output output/dq_summary
```

Inspect outputs:

```bash
ls output/good_events
ls output/bad_events
ls output/dq_summary
```

## Checkpoints

You are done when:

* The summary table includes both `pass` and `fail` statuses.
* Duplicate `evt_002` records are rejected.
* Records with missing IDs or invalid timestamps are rejected.
* Good, bad, and summary output folders are created.

## Deliverables

Submit:

* `data/events_dirty.jsonl`.
* `jobs/run_quality_checks.py`.
* Terminal output from the summary table.
* Output folders for good records, bad records, and the DQ summary.
* One paragraph explaining which quality check you think is most important and why.

## Common Issues

| Problem | Likely Cause | Fix |
| ------- | ------------ | --- |
| Duplicate check flags empty IDs strangely | Missing ID check should run before duplicate check | Keep the `when` clauses in the provided order |
| Purchase amount is always null | Amount is nested inside `payload` | Use `F.col("payload").getItem("amount")` |
| Output folder contains multiple metadata files | Spark writes job metadata | Look for files beginning with `part-` |
| Too many rows rejected | Input schema does not match the sample data | Compare your JSON fields to the schema in the script |

## Reflection Questions

Answer briefly:

* Should bad records be deleted, quarantined, or fixed automatically?
* Who should be notified when data quality checks fail?
* Which checks would you add before using this data in a dashboard?
