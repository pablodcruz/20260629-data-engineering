"""Tests for the Lab 4 PySpark transformations."""

import unittest
from pathlib import Path
import sys

SOLUTION_DIR = Path(__file__).resolve().parents[1]
# Resolve imports from the test location instead of depending on the caller's
# current working directory.
sys.path.insert(0, str(SOLUTION_DIR))

from transform_events import (
    build_spark,
    prepare_events,
    read_events,
    split_events,
    summarize_events,
)


class TransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Two local worker threads are enough for deterministic, fast unit tests.
        cls.spark = build_spark("local[2]")
        cls.spark.sparkContext.setLogLevel("ERROR")
        raw_df = read_events(cls.spark, str(SOLUTION_DIR / "data" / "events.jsonl"))
        cls.prepared_df = prepare_events(raw_df).cache()
        cls.valid_df, cls.invalid_df = split_events(cls.prepared_df)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.prepared_df.unpersist()
        cls.spark.stop()

    def test_splits_valid_and_invalid_records(self) -> None:
        self.assertEqual(self.valid_df.count(), 5)
        self.assertEqual(self.invalid_df.count(), 2)

    def test_invalid_records_keep_readable_reasons(self) -> None:
        errors_by_id = {
            row.event_id: set(row.validation_errors)
            for row in self.invalid_df.select("event_id", "validation_errors").collect()
        }

        self.assertEqual(errors_by_id[""], {"event_id is required"})
        self.assertEqual(
            errors_by_id["evt_007"],
            {"user_id is required", "event_ts is invalid"},
        )

    def test_summary_contains_expected_groups(self) -> None:
        rows = summarize_events(self.valid_df).collect()
        counts = {
            (row.event_type, row.source): (row.event_count, row.unique_users)
            for row in rows
        }

        self.assertEqual(
            counts,
            {
                ("add_to_cart", "mobile"): (1, 1),
                ("page_view", "web"): (2, 1),
                ("purchase", "web"): (1, 1),
                ("video_play", "mobile"): (1, 1),
            },
        )


if __name__ == "__main__":
    unittest.main()
