"""Tests for the Lab 5 batch-job transformations."""

import sys
import unittest
from pathlib import Path


SOLUTION_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SOLUTION_DIR / "jobs"))

from process_events import (  # noqa: E402
    build_spark,
    check_events,
    read_events,
    split_events,
    summarize_events,
)


class BatchJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = build_spark("local[2]")
        cls.spark.sparkContext.setLogLevel("ERROR")
        events_df = read_events(cls.spark, str(SOLUTION_DIR / "data" / "events.jsonl"))
        cls.checked_df = check_events(events_df).cache()
        cls.good_df, cls.bad_df = split_events(cls.checked_df)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.checked_df.unpersist()
        cls.spark.stop()

    def test_splits_four_good_and_two_bad_events(self) -> None:
        self.assertEqual(self.good_df.count(), 4)
        self.assertEqual(self.bad_df.count(), 2)

    def test_rejected_events_keep_actionable_reasons(self) -> None:
        reasons = {
            row.event_id: row.reject_reason
            for row in self.bad_df.select("event_id", "reject_reason").collect()
        }
        self.assertEqual(
            reasons,
            {
                "evt_bad_001": "missing user_id",
                "evt_bad_002": "invalid event_ts",
            },
        )

    def test_summary_contains_only_accepted_events(self) -> None:
        rows = summarize_events(self.good_df).collect()
        counts = {
            (row.event_type, row.source): (row.event_count, row.unique_users)
            for row in rows
        }
        self.assertEqual(
            counts,
            {
                ("page_view", "web"): (1, 1),
                ("purchase", "mobile"): (1, 1),
                ("purchase", "web"): (1, 1),
                ("video_play", "mobile"): (1, 1),
            },
        )


if __name__ == "__main__":
    unittest.main()
