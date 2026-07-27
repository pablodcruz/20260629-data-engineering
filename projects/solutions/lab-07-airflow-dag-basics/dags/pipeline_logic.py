"""Pure file-processing functions used by the Lab 7 Airflow tasks."""

import json
from collections import Counter
from pathlib import Path
from typing import Dict, List


REQUIRED_FIELDS = ("event_id", "event_type", "user_id")
SAMPLE_EVENTS: List[Dict[str, str]] = [
    {"event_id": "evt_001", "event_type": "page_view", "user_id": "user_101"},
    {"event_id": "evt_002", "event_type": "video_play", "user_id": "user_102"},
    {"event_id": "evt_003", "event_type": "purchase", "user_id": "user_103"},
]


def generate_events(events_path: Path) -> str:
    """Write the deterministic sample events and return their shared path."""
    events_path.parent.mkdir(parents=True, exist_ok=True)
    with events_path.open("w", encoding="utf-8") as output_file:
        for event in SAMPLE_EVENTS:
            output_file.write(json.dumps(event, sort_keys=True) + "\n")
    print(f"Wrote {len(SAMPLE_EVENTS)} events to {events_path}")
    return str(events_path)


def validate_events(events_path: Path) -> str:
    """Require a non-empty JSON Lines file with the shared event fields."""
    if not events_path.is_file():
        raise FileNotFoundError(f"Missing event file: {events_path}")

    event_count = 0
    with events_path.open("r", encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, start=1):
            event = json.loads(line)
            for field in REQUIRED_FIELDS:
                if not event.get(field):
                    raise ValueError(
                        f"Line {line_number} is missing required field {field}: {event}"
                    )
            event_count += 1

    if event_count == 0:
        raise ValueError(f"Event file contains no events: {events_path}")
    print(f"Validated {event_count} events in {events_path}")
    return str(events_path)


def summarize_events(events_path: Path, summary_path: Path) -> str:
    """Count validated events by type and write a deterministic JSON summary."""
    counts: Counter[str] = Counter()
    with events_path.open("r", encoding="utf-8") as input_file:
        for line in input_file:
            event = json.loads(line)
            counts[event["event_type"]] += 1

    summary = {
        "total_events": sum(counts.values()),
        "events_by_type": dict(sorted(counts.items())),
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    print(f"Wrote summary to {summary_path}")
    return str(summary_path)
