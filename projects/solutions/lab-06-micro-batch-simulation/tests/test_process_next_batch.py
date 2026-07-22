"""Tests for Lab 6 batch selection, state, and Spark aggregation."""

import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path


SOLUTION_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SOLUTION_DIR / "jobs"))

from process_next_batch import (  # noqa: E402
    build_spark,
    find_next_batch,
    load_processed_batches,
    main,
    mark_processed,
    summarize_batch,
)


class StateTests(unittest.TestCase):
    def test_missing_state_file_means_nothing_is_processed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_file = Path(temp_dir) / "processed_batches.txt"
            self.assertEqual(load_processed_batches(state_file), set())

    def test_mark_processed_is_sorted_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_file = Path(temp_dir) / "state" / "processed_batches.txt"
            mark_processed(state_file, "batch_002")
            mark_processed(state_file, "batch_001")
            mark_processed(state_file, "batch_002")
            self.assertEqual(
                state_file.read_text(encoding="utf-8"),
                "batch_001\nbatch_002\n",
            )

    def test_find_next_batch_uses_sorted_unprocessed_order(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            incoming_dir = Path(temp_dir)
            (incoming_dir / "batch_003").mkdir()
            (incoming_dir / "batch_001").mkdir()
            (incoming_dir / "notes").mkdir()
            next_batch = find_next_batch(incoming_dir, {"batch_001"})
            self.assertIsNotNone(next_batch)
            self.assertEqual(next_batch.name, "batch_003")

    def test_main_exits_without_starting_spark_when_all_batches_are_done(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            incoming_dir = root / "incoming"
            state_dir = root / "state"
            (incoming_dir / "batch_001").mkdir(parents=True)
            mark_processed(state_dir / "processed_batches.txt", "batch_001")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                status = main(
                    [
                        "--incoming-dir",
                        str(incoming_dir),
                        "--output-dir",
                        str(root / "output"),
                        "--state-dir",
                        str(state_dir),
                    ]
                )
            self.assertEqual(status, 0)
            self.assertIn("No new batches to process.", output.getvalue())


class SparkSummaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = build_spark("local[2]")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_batch_summary_counts_event_type_and_source(self) -> None:
        summary_df = summarize_batch(
            self.spark,
            SOLUTION_DIR / "incoming" / "batch_002",
        )
        counts = {
            (row.event_type, row.source): row.event_count
            for row in summary_df.collect()
        }
        self.assertEqual(
            counts,
            {
                ("page_view", "web"): 1,
                ("purchase", "web"): 1,
            },
        )


if __name__ == "__main__":
    unittest.main()
