"""Process exactly one unprocessed StreamFlow event batch."""

import argparse
import csv
import os
from pathlib import Path
from typing import Optional, Sequence, Set

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), nullable=True),
        T.StructField("event_type", T.StringType(), nullable=True),
        T.StructField("user_id", T.StringType(), nullable=True),
        T.StructField("event_ts", T.StringType(), nullable=True),
        T.StructField("source", T.StringType(), nullable=True),
        T.StructField(
            "payload",
            T.MapType(T.StringType(), T.StringType()),
            nullable=True,
        ),
    ]
)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Read runtime directories supplied to spark-submit."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--incoming-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--state-dir", required=True)
    return parser.parse_args(argv)


def load_processed_batches(state_file: Path) -> Set[str]:
    """Return the non-empty batch names recorded in the state file."""
    if not state_file.is_file():
        return set()
    return {
        line.strip()
        for line in state_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def mark_processed(state_file: Path, batch_name: str) -> None:
    """Record a batch once using an atomic replace instead of an append."""
    processed_batches = load_processed_batches(state_file)
    processed_batches.add(batch_name)
    state_file.parent.mkdir(parents=True, exist_ok=True)

    # Writing a complete temporary file and replacing the old state prevents a
    # crash during writing from leaving behind a partially appended batch name.
    temporary_file = state_file.with_name(f".{state_file.name}.tmp")
    temporary_file.write_text(
        "".join(f"{name}\n" for name in sorted(processed_batches)),
        encoding="utf-8",
    )
    temporary_file.replace(state_file)


def find_next_batch(incoming_dir: Path, processed_batches: Set[str]) -> Optional[Path]:
    """Select the first lexicographically sorted unprocessed batch directory."""
    batch_dirs = sorted(
        path
        for path in incoming_dir.iterdir()
        if path.is_dir() and path.name.startswith("batch_")
    )
    return next(
        (path for path in batch_dirs if path.name not in processed_batches),
        None,
    )


def build_spark(master: Optional[str] = None) -> SparkSession:
    """Create a UTC Spark session, optionally with a test-only local master."""
    builder = (
        SparkSession.builder.appName("Lab06MicroBatchSimulation")
        .config("spark.sql.session.timeZone", "UTC")
    )
    # spark-submit selects the real master. Tests use local[2] for isolation.
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def summarize_batch(spark: SparkSession, batch_dir: Path) -> DataFrame:
    """Read one batch and count events by event type and source."""
    json_files = sorted(batch_dir.glob("*.jsonl"))
    if not json_files:
        raise ValueError(f"batch contains no JSON Lines files: {batch_dir}")

    batch_df = (
        spark.read.schema(EVENT_SCHEMA)
        .json([str(path) for path in json_files])
        .withColumn("event_time", F.to_timestamp("event_ts"))
    )
    return (
        batch_df.groupBy("event_type", "source")
        .agg(F.count("*").alias("event_count"))
        .orderBy("event_type", "source")
    )


def write_small_csv_portably(dataframe: DataFrame, output_path: Path) -> None:
    """Write the tiny summary without Hadoop when native Windows needs it."""
    if output_path.exists() and not output_path.is_dir():
        raise ValueError(f"output path exists but is not a directory: {output_path}")
    output_path.mkdir(parents=True, exist_ok=True)

    # Remove only files owned by this fallback, never the whole supplied path.
    for old_part in output_path.glob("part-*-local.csv"):
        old_part.unlink()
    (output_path / "_SUCCESS").unlink(missing_ok=True)

    with (output_path / "part-00000-local.csv").open(
        "w", encoding="utf-8", newline=""
    ) as output_file:
        writer = csv.writer(output_file, lineterminator="\n")
        writer.writerow(dataframe.columns)
        writer.writerows(
            [row[column] for column in dataframe.columns]
            for row in dataframe.collect()
        )
    (output_path / "_SUCCESS").touch()


def write_summary(dataframe: DataFrame, output_path: Path) -> str:
    """Write one batch's CSV dataset and return the writer implementation."""
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        write_small_csv_portably(dataframe, output_path)
        return "portable Windows fallback"

    try:
        (
            # A single part file is convenient only because each lab batch is
            # tiny. Real pipelines retain partitions for parallel output.
            dataframe.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(output_path))
        )
        return "Spark CSV writer"
    except Py4JJavaError as exc:
        if "HADOOP_HOME and hadoop.home.dir are unset" not in str(exc):
            raise
        write_small_csv_portably(dataframe, output_path)
        return "portable Windows fallback"


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Process one batch, write output, then update state after success."""
    args = parse_args(argv)
    incoming_dir = Path(args.incoming_dir)
    output_dir = Path(args.output_dir)
    state_file = Path(args.state_dir) / "processed_batches.txt"

    if not incoming_dir.is_dir():
        print(f"incoming directory not found: {incoming_dir}")
        return 2

    processed_batches = load_processed_batches(state_file)
    next_batch = find_next_batch(incoming_dir, processed_batches)
    if next_batch is None:
        print("No new batches to process.")
        return 0

    print(f"Processing {next_batch.name}")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary_df = summarize_batch(spark, next_batch)
        summary_df.show(truncate=False)
        batch_output = output_dir / next_batch.name
        writer_name = write_summary(summary_df, batch_output)
    finally:
        spark.stop()

    # State moves forward only after Spark successfully commits the output. A
    # failed run remains eligible for a safe retry using overwrite mode.
    mark_processed(state_file, next_batch.name)
    print(f"Finished {next_batch.name}")
    print(f"Wrote output to {batch_output} using {writer_name}")
    print(f"Updated state file at {state_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
