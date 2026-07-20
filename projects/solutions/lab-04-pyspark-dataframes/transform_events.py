"""Clean StreamFlow events and create an event-type/source summary with Spark."""

import argparse
import csv
import os
from pathlib import Path
from datetime import datetime

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


ALLOWED_EVENT_TYPES = ("page_view", "video_play", "video_pause", "add_to_cart", "purchase")
ALLOWED_SOURCES = ("web", "mobile", "api", "system")

# An explicit schema makes input behavior predictable. Without one, Spark scans
# the data to infer types, and one unusual record can change a column's type.
EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), nullable=True),
        T.StructField("event_type", T.StringType(), nullable=True),
        T.StructField("user_id", T.StringType(), nullable=True),
        T.StructField("event_ts", T.StringType(), nullable=True),
        T.StructField("source", T.StringType(), nullable=True),
        # MapType works for these small, string-valued payload examples. A
        # production schema might use a typed struct per event type instead.
        T.StructField(
            "payload",
            T.MapType(T.StringType(), T.StringType()),
            nullable=True,
        ),
    ]
)


def build_spark(master: str = "local[*]") -> SparkSession:
    """Create a local Spark session with deterministic UTC timestamp handling."""
    return (
        SparkSession.builder.appName("Lab04PySparkDataFrames")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_events(spark: SparkSession, input_path: str) -> DataFrame:
    """Read JSON Lines events using the declared schema."""
    return spark.read.schema(EVENT_SCHEMA).json(input_path)


def prepare_events(raw_df: DataFrame) -> DataFrame:
    """Parse timestamps and attach an array explaining every invalid field."""
    return (
        raw_df.withColumn("event_time", F.to_timestamp("event_ts"))
        .withColumn("ingest_date", F.to_date("event_time"))
        .withColumn(
            "validation_errors",
            # Each condition returns either an error string or null. Compacting
            # the array removes the nulls and preserves all applicable errors.
            F.array_compact(
                F.array(
                    F.when(
                        F.col("event_id").isNull()
                        | (F.length(F.trim("event_id")) == 0),
                        F.lit("event_id is required"),
                    ),
                    F.when(
                        F.col("event_type").isNull()
                        | ~F.col("event_type").isin(*ALLOWED_EVENT_TYPES),
                        F.lit("event_type is not allowed"),
                    ),
                    F.when(
                        F.col("user_id").isNull()
                        | (F.length(F.trim("user_id")) == 0),
                        F.lit("user_id is required"),
                    ),
                    F.when(
                        F.col("event_time").isNull(),
                        F.lit("event_ts is invalid"),
                    ),
                    F.when(
                        F.col("source").isNull()
                        | ~F.col("source").isin(*ALLOWED_SOURCES),
                        F.lit("source is not allowed"),
                    ),
                    F.when(
                        F.col("payload").isNull(),
                        F.lit("payload is required"),
                    ),
                )
            ),
        )
    )


def split_events(prepared_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return valid and invalid DataFrames without subtracting full rows."""
    # Filtering on validation_errors is clearer and cheaper than exceptAll: each
    # input row is classified directly, and invalid rows retain their reasons.
    valid_df = prepared_df.filter(F.size("validation_errors") == 0)
    invalid_df = prepared_df.filter(F.size("validation_errors") > 0)
    return valid_df, invalid_df


def summarize_events(valid_df: DataFrame) -> DataFrame:
    """Aggregate valid events by event type and source."""
    return (
        valid_df.groupBy("event_type", "source")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.min("event_time").alias("first_event_time"),
            F.max("event_time").alias("last_event_time"),
        )
        .orderBy("event_type", "source")
    )


def format_csv_value(value: object) -> object:
    """Format collected timestamps like Spark's UTC CSV writer."""
    if isinstance(value, datetime):
        return f"{value.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z"
    return value


def write_summary_portably(summary_df: DataFrame, output_path: str) -> None:
    """Write the tiny collected summary when native Windows lacks winutils."""
    output_dir = Path(output_path)
    if output_dir.exists() and not output_dir.is_dir():
        raise ValueError(f"output path exists but is not a directory: {output_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Remove only files owned by this fallback. We deliberately avoid deleting
    # the entire user-provided directory or unrelated files inside it.
    for old_part in output_dir.glob("part-*-local.csv"):
        old_part.unlink()
    (output_dir / "_SUCCESS").unlink(missing_ok=True)

    columns = summary_df.columns
    rows = summary_df.collect()
    part_path = output_dir / "part-00000-local.csv"

    with part_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file, lineterminator="\n")
        writer.writerow(columns)
        writer.writerows(
            [format_csv_value(row[column]) for column in columns] for row in rows
        )

    # Spark uses this empty marker to signal that a dataset write completed.
    (output_dir / "_SUCCESS").touch()


def write_summary(summary_df: DataFrame, output_path: str) -> str:
    """Write a headered CSV dataset and return the writer implementation used."""
    # Spark 3.5's Hadoop file writer needs winutils.exe on native Windows. Since
    # this summary contains only four aggregate rows, collecting it is safe and
    # avoids asking students to download an unofficial executable.
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        write_summary_portably(summary_df, output_path)
        return "portable Windows fallback"

    try:
        (
            # One partition creates one data file for this tiny lab. Large
            # pipelines normally keep many partitions for parallel writes.
            summary_df.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(output_path)
        )
        return "Spark CSV writer"
    except Py4JJavaError as exc:
        # A configured-but-incomplete Hadoop installation produces the same
        # Windows error. Fall back only for that known case; re-raise everything
        # else so genuine write failures are never hidden.
        if "HADOOP_HOME and hadoop.home.dir are unset" not in str(exc):
            raise
        write_summary_portably(summary_df, output_path)
        return "portable Windows fallback"


def parse_args() -> argparse.Namespace:
    """Read optional input and output paths."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_path", nargs="?", default="data/events.jsonl")
    parser.add_argument("output_path", nargs="?", default="output/event_summary")
    return parser.parse_args()


def main() -> int:
    """Run the complete read, clean, summarize, and write workflow."""
    args = parse_args()

    if not Path(args.input_path).is_file():
        print(f"input file not found: {args.input_path}")
        return 2

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df = read_events(spark, args.input_path)
        prepared_df = prepare_events(raw_df)
        valid_df, invalid_df = split_events(prepared_df)
        summary_df = summarize_events(valid_df)

        print("Raw schema:")
        raw_df.printSchema()

        print("Valid records:")
        valid_df.select(
            "event_id", "event_type", "user_id", "event_time", "source"
        ).show(truncate=False)

        print("Invalid records and reasons:")
        invalid_df.select(
            "event_id", "event_type", "user_id", "event_ts", "validation_errors"
        ).show(truncate=False)

        print("Summary:")
        summary_df.show(truncate=False)
        writer_name = write_summary(summary_df, args.output_path)
    finally:
        # Always stop the JVM-backed session, including when a transformation or
        # write fails, so the Python process can exit cleanly.
        spark.stop()

    print(f"wrote summary dataset to {args.output_path} using {writer_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
