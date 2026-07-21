"""Validate StreamFlow events and write batch summary and reject datasets."""

import argparse
import csv
import os
from pathlib import Path
from typing import Optional, Sequence

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# Declaring the input shape prevents a malformed record from changing Spark's
# inferred types and makes the job behave consistently across runs.
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

BAD_OUTPUT_COLUMNS = (
    "event_id",
    "event_type",
    "user_id",
    "event_ts",
    "source",
    "reject_reason",
)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse paths supplied by spark-submit or by an automated test."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input JSON Lines path")
    parser.add_argument(
        "--summary-output", required=True, help="Output path for summary CSV"
    )
    parser.add_argument(
        "--bad-output", required=True, help="Output path for rejected records CSV"
    )
    return parser.parse_args(argv)


def build_spark(master: Optional[str] = None) -> SparkSession:
    """Create a UTC Spark session, optionally using a test-only local master."""
    builder = (
        SparkSession.builder.appName("Lab05SparkBatchJob")
        .config("spark.sql.session.timeZone", "UTC")
    )
    # spark-submit chooses the real master. Tests supply local[2] so they do not
    # depend on a cluster or on a machine's default Spark configuration.
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def read_events(spark: SparkSession, input_path: str) -> DataFrame:
    """Read JSON Lines input using the fixed event schema."""
    return spark.read.schema(EVENT_SCHEMA).json(input_path)


def check_events(events_df: DataFrame) -> DataFrame:
    """Parse timestamps and attach the first actionable rejection reason."""
    return (
        events_df.withColumn("event_time", F.to_timestamp("event_ts"))
        .withColumn(
            "reject_reason",
            # The ordered chain gives each rejected record one stable primary
            # reason, which keeps the reject CSV straightforward to triage.
            F.when(
                F.col("event_id").isNull()
                | (F.length(F.trim("event_id")) == 0),
                F.lit("missing event_id"),
            )
            .when(
                F.col("user_id").isNull()
                | (F.length(F.trim("user_id")) == 0),
                F.lit("missing user_id"),
            )
            .when(F.col("event_time").isNull(), F.lit("invalid event_ts")),
        )
    )


def split_events(checked_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separate accepted records from records that need correction."""
    good_df = checked_df.filter(F.col("reject_reason").isNull())
    bad_df = checked_df.filter(F.col("reject_reason").isNotNull())
    return good_df, bad_df


def summarize_events(good_df: DataFrame) -> DataFrame:
    """Count accepted events and users for each type/source pair."""
    return (
        good_df.groupBy("event_type", "source")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .orderBy("event_type", "source")
    )


def write_small_csv_portably(dataframe: DataFrame, output_path: str) -> None:
    """Write a tiny collected dataset when native Windows lacks winutils."""
    output_dir = Path(output_path)
    if output_dir.exists() and not output_dir.is_dir():
        raise ValueError(f"output path exists but is not a directory: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Delete only files created by this fallback, preserving unrelated content
    # if a learner accidentally points at an existing directory.
    for old_part in output_dir.glob("part-*-local.csv"):
        old_part.unlink()
    (output_dir / "_SUCCESS").unlink(missing_ok=True)

    with (output_dir / "part-00000-local.csv").open(
        "w", encoding="utf-8", newline=""
    ) as output_file:
        writer = csv.writer(output_file, lineterminator="\n")
        writer.writerow(dataframe.columns)
        writer.writerows(
            [row[column] for column in dataframe.columns]
            for row in dataframe.collect()
        )
    (output_dir / "_SUCCESS").touch()


def write_csv_dataset(dataframe: DataFrame, output_path: str) -> str:
    """Write a headered CSV dataset and name the implementation used."""
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        write_small_csv_portably(dataframe, output_path)
        return "portable Windows fallback"

    try:
        (
            # One partition is convenient for this six-row teaching dataset.
            # Production jobs normally retain multiple output partitions.
            dataframe.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(output_path)
        )
        return "Spark CSV writer"
    except Py4JJavaError as exc:
        # Fall back only for the known native-Windows Hadoop error. Re-raising
        # every other error avoids hiding permissions or data failures.
        if "HADOOP_HOME and hadoop.home.dir are unset" not in str(exc):
            raise
        write_small_csv_portably(dataframe, output_path)
        return "portable Windows fallback"


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the complete batch job."""
    args = parse_args(argv)
    if not Path(args.input).is_file():
        print(f"input file not found: {args.input}")
        return 2

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        events_df = read_events(spark, args.input)
        checked_df = check_events(events_df)
        good_df, bad_df = split_events(checked_df)
        summary_df = summarize_events(good_df)
        bad_output_df = bad_df.select(*BAD_OUTPUT_COLUMNS).orderBy("event_id")

        print("Summary output:")
        summary_df.show(truncate=False)
        print("Rejected records:")
        bad_output_df.show(truncate=False)

        summary_writer = write_csv_dataset(summary_df, args.summary_output)
        bad_writer = write_csv_dataset(bad_output_df, args.bad_output)
    finally:
        # Always close the JVM-backed session, including after a failed write.
        spark.stop()

    print(f"wrote summary dataset using {summary_writer}")
    print(f"wrote rejected-record dataset using {bad_writer}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
