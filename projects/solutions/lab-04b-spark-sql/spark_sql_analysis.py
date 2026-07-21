"""Analyze StreamFlow events with Spark SQL and write partitioned output."""

import argparse
from collections import defaultdict
from datetime import date, datetime
import json
import os
from pathlib import Path

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# Explicit schemas prevent inference from changing when a file contains an
# unusual null or numeric value.
EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), nullable=False),
        T.StructField("event_type", T.StringType(), nullable=False),
        T.StructField("user_id", T.StringType(), nullable=False),
        T.StructField("event_ts", T.StringType(), nullable=False),
        T.StructField("source", T.StringType(), nullable=False),
        T.StructField("session_id", T.StringType(), nullable=False),
        T.StructField("amount", T.DoubleType(), nullable=True),
    ]
)

USER_SCHEMA = T.StructType(
    [
        T.StructField("user_id", T.StringType(), nullable=False),
        T.StructField("plan", T.StringType(), nullable=False),
        T.StructField("region", T.StringType(), nullable=False),
    ]
)

# Keeping SQL text in one place makes each question independently testable.
ANALYSIS_QUERIES = {
    "web_events": """
        SELECT event_id, event_type, user_id, event_time, source
        FROM events
        WHERE source = 'web'
        ORDER BY event_time
    """,
    "event_summary": """
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
    """,
    "purchase_revenue": """
        SELECT
            COALESCE(u.plan, 'unknown') AS plan,
            COALESCE(u.region, 'unknown') AS region,
            COUNT(*) AS purchase_count,
            ROUND(SUM(e.amount), 2) AS revenue
        FROM events e
        LEFT JOIN users u ON e.user_id = u.user_id
        WHERE e.event_type = 'purchase'
        GROUP BY COALESCE(u.plan, 'unknown'), COALESCE(u.region, 'unknown')
        ORDER BY revenue DESC
    """,
    "missing_users": """
        SELECT e.event_id, e.user_id, e.event_type, e.source
        FROM events e
        LEFT ANTI JOIN users u ON e.user_id = u.user_id
        ORDER BY e.event_id
    """,
    "non_premium_purchasers": """
        SELECT user_id
        FROM events
        WHERE event_type = 'purchase'

        EXCEPT

        SELECT user_id
        FROM users
        WHERE plan = 'premium'
    """,
}

QUERY_TITLES = {
    "web_events": "1. Select and filter web events",
    "event_summary": "2. Aggregate events by type and source",
    "purchase_revenue": "3. Purchase revenue by plan and region",
    "missing_users": "4. Events with missing user lookup records",
    "non_premium_purchasers": "5. Users who purchased but are not premium",
}


def build_spark(master: str = "local[*]") -> SparkSession:
    """Create a local Spark session with deterministic UTC timestamps."""
    return (
        SparkSession.builder.appName("Lab04BSparkSQL")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_data(
    spark: SparkSession, events_path: str, users_path: str
) -> tuple[DataFrame, DataFrame]:
    """Load the two datasets and derive SQL-friendly event columns."""
    events_df = (
        spark.read.schema(EVENT_SCHEMA)
        .json(events_path)
        .withColumn("event_time", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_time"))
    )
    users_df = spark.read.schema(USER_SCHEMA).json(users_path)
    return events_df, users_df


def register_views(events_df: DataFrame, users_df: DataFrame) -> None:
    """Expose DataFrames as session-scoped SQL tables."""
    events_df.createOrReplaceTempView("events")
    users_df.createOrReplaceTempView("users")


def run_analysis_queries(spark: SparkSession) -> dict[str, DataFrame]:
    """Build one lazy DataFrame for each analytics question."""
    return {
        name: spark.sql(query.strip()) for name, query in ANALYSIS_QUERIES.items()
    }


def build_enriched_events(spark: SparkSession) -> DataFrame:
    """Join events to user attributes while retaining unmatched events."""
    return spark.sql(
        """
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
        LEFT JOIN users u ON e.user_id = u.user_id
        """
    )


def query_cached_summary(spark: SparkSession) -> DataFrame:
    """Aggregate the cached enriched view by date and plan."""
    return spark.sql(
        """
        SELECT event_date, plan, COUNT(*) AS event_count
        FROM enriched_events
        GROUP BY event_date, plan
        ORDER BY event_date, plan
        """
    )


def show_section(title: str, dataframe: DataFrame) -> None:
    """Print a consistent heading and an untruncated Spark result table."""
    print(f"\n=== {title} ===")
    dataframe.show(truncate=False)


def json_value(value: object) -> object:
    """Convert collected date/time values to JSON-compatible UTC text."""
    if isinstance(value, datetime):
        return f"{value.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z"
    if isinstance(value, date):
        return value.isoformat()
    return value


def write_partitioned_portably(enriched_df: DataFrame, output_path: str) -> None:
    """Create a teaching-scale partition layout without Hadoop winutils."""
    output_dir = Path(output_path)
    if output_dir.exists() and not output_dir.is_dir():
        raise ValueError(f"output path exists but is not a directory: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Remove only part files owned by this fallback. Do not recursively delete a
    # caller-supplied directory or unrelated evidence a student may have saved.
    for old_part in output_dir.glob("event_date=*/part-*-local.jsonl"):
        old_part.unlink()
    (output_dir / "_SUCCESS").unlink(missing_ok=True)

    partitioned_rows: dict[str, list[dict[str, object]]] = defaultdict(list)
    data_columns = [column for column in enriched_df.columns if column != "event_date"]

    # Collecting is acceptable only because this fixture has ten rows. A real
    # pipeline must use Spark's distributed writer instead.
    for row in enriched_df.collect():
        partition_name = row.event_date.isoformat()
        partitioned_rows[partition_name].append(
            {column: json_value(row[column]) for column in data_columns}
        )

    for partition_name, records in sorted(partitioned_rows.items()):
        partition_dir = output_dir / f"event_date={partition_name}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        part_path = partition_dir / "part-00000-local.jsonl"
        with part_path.open("w", encoding="utf-8", newline="\n") as output_file:
            for record in records:
                output_file.write(json.dumps(record, separators=(",", ":")) + "\n")

    (output_dir / "_SUCCESS").touch()


def write_partitioned_output(enriched_df: DataFrame, output_path: str) -> str:
    """Write partitioned Parquet when supported, with a safe Windows fallback."""
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        write_partitioned_portably(enriched_df, output_path)
        return "portable Windows JSONL fallback"

    try:
        (
            enriched_df.write.mode("overwrite")
            .partitionBy("event_date")
            .parquet(output_path)
        )
        return "Spark partitioned Parquet writer"
    except Py4JJavaError as exc:
        message = str(exc)
        if not any(
            marker in message
            for marker in ("HADOOP_HOME and hadoop.home.dir are unset", "winutils.exe")
        ):
            raise
        write_partitioned_portably(enriched_df, output_path)
        return "portable Windows JSONL fallback"


def parse_args() -> argparse.Namespace:
    """Read optional data and output paths."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", default="data/events.jsonl")
    parser.add_argument("--users", default="data/users.jsonl")
    parser.add_argument("--output", default="output/enriched_events_by_date")
    return parser.parse_args()


def main() -> int:
    """Run all SQL examples, caching exercise, and partitioned write."""
    args = parse_args()
    for input_path in (args.events, args.users):
        if not Path(input_path).is_file():
            print(f"input file not found: {input_path}")
            return 2

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    enriched_df: DataFrame | None = None

    try:
        events_df, users_df = load_data(spark, args.events, args.users)
        register_views(events_df, users_df)

        for name, dataframe in run_analysis_queries(spark).items():
            show_section(QUERY_TITLES[name], dataframe)

        enriched_df = build_enriched_events(spark).cache()
        enriched_df.createOrReplaceTempView("enriched_events")
        print(f"\nCached enriched_events row count: {enriched_df.count()}")
        show_section("6. Query the cached enriched view", query_cached_summary(spark))

        writer_name = write_partitioned_output(enriched_df, args.output)
    finally:
        if enriched_df is not None:
            enriched_df.unpersist()
        spark.stop()

    print(f"wrote partitioned output to {args.output} using {writer_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
