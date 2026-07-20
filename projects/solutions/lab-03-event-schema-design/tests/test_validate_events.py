"""Unit and integration tests for the Lab 3 event validator."""

import unittest
from pathlib import Path
import sys

SOLUTION_DIR = Path(__file__).resolve().parents[1]
# Make the solution module importable whether tests run from this lab folder or
# from the repository root as part of a broader validation command.
sys.path.insert(0, str(SOLUTION_DIR))

from validate_events import is_timestamp, validate_event, validate_file, validate_lines


def valid_page_view() -> dict[str, object]:
    """Create a fresh valid event so tests can safely modify it."""
    return {
        "event_id": "evt_test_001",
        "event_type": "page_view",
        "user_id": "user_test_001",
        "event_ts": "2026-06-30T14:00:00Z",
        "source": "web",
        "payload": {"page": "/home"},
    }


class TimestampTests(unittest.TestCase):
    def test_accepts_timezone_aware_timestamps(self) -> None:
        self.assertTrue(is_timestamp("2026-06-30T14:00:00Z"))
        self.assertTrue(is_timestamp("2026-06-30T10:00:00-04:00"))

    def test_rejects_ambiguous_or_malformed_timestamps(self) -> None:
        self.assertFalse(is_timestamp("2026-06-30T14:00:00"))
        self.assertFalse(is_timestamp("not-a-timestamp"))


class EventValidationTests(unittest.TestCase):
    def test_accepts_a_valid_event(self) -> None:
        self.assertEqual(validate_event(valid_page_view()), [])

    def test_reports_multiple_errors_together(self) -> None:
        event = valid_page_view()
        event["user_id"] = ""
        event["source"] = "browser"
        event["payload"] = {"page": ""}

        errors = validate_event(event)

        self.assertIn("user_id must be a non-empty string", errors)
        self.assertIn("source is not allowed", errors)
        self.assertIn("page_view payload.page must be a non-empty string", errors)

    def test_detects_duplicate_ids_across_lines(self) -> None:
        line = (
            '{"event_id":"evt_duplicate","event_type":"page_view",'
            '"user_id":"user_1","event_ts":"2026-06-30T14:00:00Z",'
            '"source":"web","payload":{"page":"/home"}}'
        )

        results = validate_lines([line, line])

        self.assertTrue(results[0].is_valid)
        self.assertIn("duplicate event_id: evt_duplicate", results[1].errors)


class FixtureTests(unittest.TestCase):
    def test_valid_fixture_passes(self) -> None:
        results = validate_file(SOLUTION_DIR / "data" / "valid_events.jsonl")
        self.assertEqual(len(results), 3)
        self.assertTrue(all(result.is_valid for result in results))

    def test_invalid_fixture_fails(self) -> None:
        results = validate_file(SOLUTION_DIR / "data" / "invalid_events.jsonl")
        self.assertEqual(len(results), 3)
        self.assertTrue(all(not result.is_valid for result in results))


if __name__ == "__main__":
    unittest.main()
