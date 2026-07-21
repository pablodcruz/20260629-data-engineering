"""Tests for the Lab 4B Spark SQL queries and partitioned fallback writer."""

import json
from pathlib import Path
import sys
import tempfile
import unittest


SOLUTION_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SOLUTION_DIR))

from spark_sql_analysis import (
    build_enriched_events,
    build_spark,
    load_data,
    register_views,
    run_analysis_queries,
    write_partitioned_portably,
)


class SparkSqlAnalysisTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = build_spark("local[2]")
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.events_df, cls.users_df = load_data(
            cls.spark,
            str(SOLUTION_DIR / "data" / "events.jsonl"),
            str(SOLUTION_DIR / "data" / "users.jsonl"),
        )
        register_views(cls.events_df, cls.users_df)
        cls.results = run_analysis_queries(cls.spark)
        cls.enriched_df = build_enriched_events(cls.spark).cache()
        cls.enriched_df.count()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.enriched_df.unpersist()
        cls.spark.stop()

    def test_selects_five_web_events_in_time_order(self) -> None:
        rows = self.results["web_events"].collect()
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[0].event_id, "evt_001")
        self.assertEqual(rows[-1].event_id, "evt_008")

    def test_aggregates_page_views(self) -> None:
        groups = {
            (row.event_type, row.source): (row.event_count, row.unique_users)
            for row in self.results["event_summary"].collect()
        }
        self.assertEqual(groups[("page_view", "web")], (3, 2))
        self.assertEqual(len(groups), 6)

    def test_left_join_preserves_unknown_purchase_revenue(self) -> None:
        revenue = {
            (row.plan, row.region): (row.purchase_count, row.revenue)
            for row in self.results["purchase_revenue"].collect()
        }
        self.assertEqual(revenue[("unknown", "unknown")], (1, 49.99))
        self.assertEqual(sum(count for count, _ in revenue.values()), 4)

    def test_anti_join_finds_the_missing_user(self) -> None:
        rows = self.results["missing_users"].collect()
        self.assertEqual([(row.event_id, row.user_id) for row in rows], [("evt_010", "user_999")])

    def test_except_returns_non_premium_purchasers(self) -> None:
        user_ids = {row.user_id for row in self.results["non_premium_purchasers"].collect()}
        self.assertEqual(user_ids, {"user_104", "user_999"})

    def test_portable_writer_creates_two_partition_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "enriched"
            write_partitioned_portably(self.enriched_df, str(output_path))

            parts = sorted(output_path.glob("event_date=*/part-*-local.jsonl"))
            records = [
                json.loads(line)
                for part in parts
                for line in part.read_text(encoding="utf-8").splitlines()
            ]

            self.assertEqual(len(parts), 2)
            self.assertEqual(len(records), 10)
            self.assertTrue((output_path / "_SUCCESS").is_file())


if __name__ == "__main__":
    unittest.main()
