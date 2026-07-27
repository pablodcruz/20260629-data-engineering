"""Tests for the file-processing logic behind the Lab 7 DAG."""

import json
import sys
import tempfile
import unittest
from pathlib import Path


SOLUTION_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SOLUTION_DIR / "dags"))

from pipeline_logic import (  # noqa: E402
    SAMPLE_EVENTS,
    generate_events,
    summarize_events,
    validate_events,
)


class PipelineLogicTests(unittest.TestCase):
    def test_generate_events_writes_the_three_samples(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            events_path = Path(temp_dir) / "events.jsonl"
            self.assertEqual(generate_events(events_path), str(events_path))
            events = [
                json.loads(line)
                for line in events_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(events, SAMPLE_EVENTS)

    def test_validate_events_accepts_generated_data(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            events_path = Path(temp_dir) / "events.jsonl"
            generate_events(events_path)
            self.assertEqual(validate_events(events_path), str(events_path))

    def test_validate_events_rejects_a_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(FileNotFoundError, "Missing event file"):
                validate_events(Path(temp_dir) / "missing.jsonl")

    def test_validate_events_reports_the_line_and_missing_field(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            events_path = Path(temp_dir) / "events.jsonl"
            events_path.write_text(
                '{"event_id":"evt_bad","event_type":"page_view"}\n',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "Line 1.*user_id"):
                validate_events(events_path)

    def test_summarize_events_writes_expected_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            events_path = root / "events.jsonl"
            summary_path = root / "summary.json"
            generate_events(events_path)
            validate_events(events_path)
            self.assertEqual(
                summarize_events(events_path, summary_path),
                str(summary_path),
            )
            self.assertEqual(
                json.loads(summary_path.read_text(encoding="utf-8")),
                {
                    "total_events": 3,
                    "events_by_type": {
                        "page_view": 1,
                        "purchase": 1,
                        "video_play": 1,
                    },
                },
            )


if __name__ == "__main__":
    unittest.main()
