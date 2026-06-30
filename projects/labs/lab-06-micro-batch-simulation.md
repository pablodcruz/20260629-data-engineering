# Lab 6 - Micro-Batch Simulation

## Objective

Simulate streaming behavior by processing small batches of files over time.

## Scenario

Many streaming systems process data in small chunks instead of one giant file.
In this lab, you will imitate that pattern by dropping batches of event files into an input folder and running a Spark job that processes only the next unprocessed batch.

## What You Will Build

You will create:

* Multiple input batch folders.
* A Spark job that tracks processed batches.
* One output folder per processed batch.
* A simple state file that prevents reprocessing.

## Prerequisites

* PySpark available locally.
* Lab 5 completed or equivalent `spark-submit` setup.

## Suggested Folder

From your lab workspace:

```bash
mkdir -p lab-06-micro-batch/jobs
mkdir -p lab-06-micro-batch/incoming/batch_001
mkdir -p lab-06-micro-batch/incoming/batch_002
mkdir -p lab-06-micro-batch/incoming/batch_003
cd lab-06-micro-batch
touch jobs/process_next_batch.py
touch incoming/batch_001/events.jsonl
touch incoming/batch_002/events.jsonl
touch incoming/batch_003/events.jsonl
```

## Input Batches

Create `incoming/batch_001/events.jsonl`:

```json
{"event_id":"evt_001","event_type":"page_view","user_id":"user_101","event_ts":"2026-06-30T14:00:00Z","source":"web","payload":{"page":"/home"}}
{"event_id":"evt_002","event_type":"video_play","user_id":"user_102","event_ts":"2026-06-30T14:01:00Z","source":"mobile","payload":{"video_id":"vid_001"}}
```

Create `incoming/batch_002/events.jsonl`:

```json
{"event_id":"evt_003","event_type":"purchase","user_id":"user_103","event_ts":"2026-06-30T14:05:00Z","source":"web","payload":{"amount":"19.99"}}
{"event_id":"evt_004","event_type":"page_view","user_id":"user_101","event_ts":"2026-06-30T14:08:00Z","source":"web","payload":{"page":"/account"}}
```

Create `incoming/batch_003/events.jsonl`:

```json
{"event_id":"evt_005","event_type":"add_to_cart","user_id":"user_104","event_ts":"2026-06-30T14:10:00Z","source":"mobile","payload":{"sku":"sku_001"}}
{"event_id":"evt_006","event_type":"purchase","user_id":"user_104","event_ts":"2026-06-30T14:12:00Z","source":"mobile","payload":{"amount":"9.99"}}
```

## Micro-Batch Job

Create `jobs/process_next_batch.py`:

```python
import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def parse_args():
    parser = argparse.ArgumentParser(description="Process the next unprocessed event batch.")
    parser.add_argument("--incoming-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--state-dir", required=True)
    return parser.parse_args()


def load_processed_batches(state_file):
    if not state_file.exists():
        return set()

    return {
        line.strip()
        for line in state_file.read_text().splitlines()
        if line.strip()
    }


def mark_processed(state_file, batch_name):
    state_file.parent.mkdir(parents=True, exist_ok=True)

    with state_file.open("a", encoding="utf-8") as handle:
        handle.write(f"{batch_name}\n")


def find_next_batch(incoming_dir, processed_batches):
    batch_dirs = sorted(
        path
        for path in incoming_dir.iterdir()
        if path.is_dir() and path.name.startswith("batch_")
    )

    for batch_dir in batch_dirs:
        if batch_dir.name not in processed_batches:
            return batch_dir

    return None


def build_spark():
    return (
        SparkSession.builder
        .appName("Lab06MicroBatchSimulation")
        .getOrCreate()
    )


def main():
    args = parse_args()
    incoming_dir = Path(args.incoming_dir)
    output_dir = Path(args.output_dir)
    state_dir = Path(args.state_dir)
    state_file = state_dir / "processed_batches.txt"

    processed_batches = load_processed_batches(state_file)
    next_batch = find_next_batch(incoming_dir, processed_batches)

    if next_batch is None:
        print("No new batches to process.")
        return

    print(f"Processing {next_batch.name}")

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

    batch_df = (
        spark.read
        .schema(schema)
        .json(str(next_batch))
        .withColumn("event_time", F.to_timestamp("event_ts"))
    )

    summary_df = (
        batch_df
        .groupBy("event_type", "source")
        .agg(F.count("*").alias("event_count"))
        .orderBy("event_type", "source")
    )

    summary_df.show(truncate=False)

    batch_output = output_dir / next_batch.name

    (
        summary_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(batch_output))
    )

    spark.stop()
    mark_processed(state_file, next_batch.name)
    print(f"Finished {next_batch.name}")
    print(f"Wrote output to {batch_output}")
    print(f"Updated state file at {state_file}")


if __name__ == "__main__":
    main()
```

## Run the Simulation

Run the job once:

```bash
spark-submit jobs/process_next_batch.py --incoming-dir incoming --output-dir output --state-dir _state
```

Run it a second time:

```bash
spark-submit jobs/process_next_batch.py --incoming-dir incoming --output-dir output --state-dir _state
```

Run it a third time:

```bash
spark-submit jobs/process_next_batch.py --incoming-dir incoming --output-dir output --state-dir _state
```

Run it a fourth time:

```bash
spark-submit jobs/process_next_batch.py --incoming-dir incoming --output-dir output --state-dir _state
```

The fourth run should say there are no new batches to process.

Inspect outputs and state:

```bash
ls output
cat _state/processed_batches.txt
```

## Add a New Batch

Create `incoming/batch_004/events.jsonl`:

```json
{"event_id":"evt_007","event_type":"video_pause","user_id":"user_102","event_ts":"2026-06-30T14:15:00Z","source":"mobile","payload":{"video_id":"vid_001"}}
```

Run the job again:

```bash
spark-submit jobs/process_next_batch.py --incoming-dir incoming --output-dir output --state-dir _state
```

## Checkpoints

You are done when:

* Each run processes only one new batch.
* The state file lists processed batch names.
* Each processed batch has its own output folder.
* A new batch added later can still be detected and processed.

## Deliverables

Submit:

* `jobs/process_next_batch.py`.
* The `incoming` batch files.
* A screenshot or copied output showing four runs.
* The final `_state/processed_batches.txt` content.
* One paragraph explaining how this resembles stream processing.

## Common Issues

| Problem | Likely Cause | Fix |
| ------- | ------------ | --- |
| Same batch keeps processing | State file is not being written | Check `_state/processed_batches.txt` and folder permissions |
| No batches found | Wrong incoming directory | Confirm `incoming/batch_001/events.jsonl` exists |
| Output missing | Spark failed before writing | Check terminal logs above the final error |
| Need to restart the lab | Old state file still exists | Delete `_state/processed_batches.txt` and rerun |
