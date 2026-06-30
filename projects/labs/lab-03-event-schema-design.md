# Lab 3 - Event Schema Design

## Objective

Design a clear event schema before building processing logic.

## Scenario

Spark jobs are easier to write when event data has predictable fields and types.
In this lab, you will define the shape of StreamFlow events, create valid and invalid examples, and write a small validator.

## What You Will Build

You will create:

* An event schema table.
* Valid JSON event examples.
* Invalid JSON event examples.
* A Python validator that checks required fields and simple data rules.

## Suggested Folder

From your lab workspace:

```bash
mkdir -p lab-03-schema/data
cd lab-03-schema
touch schema.md data/valid_events.jsonl data/invalid_events.jsonl validate_events.py
```

## Event Theme

Use this event theme unless your instructor assigns a different one:

StreamFlow tracks activity in a digital media app.
Users can view pages, play videos, pause videos, add items to a cart, or purchase a subscription.

## Required Schema

Create `schema.md` and include this table:

```markdown
| Field | Type | Required | Example | Rule |
| ----- | ---- | -------- | ------- | ---- |
| event_id | string | yes | evt_001 | Must be unique and non-empty |
| event_type | string | yes | page_view | Must be one of the allowed event types |
| user_id | string | yes | user_123 | Must be non-empty |
| event_ts | string | yes | 2026-06-30T14:30:00Z | Must be ISO-like timestamp text |
| source | string | yes | web | Must be web, mobile, api, or system |
| payload | object | yes | {"page":"/home"} | Event-specific details |
```

Allowed event types:

* `page_view`
* `video_play`
* `video_pause`
* `add_to_cart`
* `purchase`

Allowed sources:

* `web`
* `mobile`
* `api`
* `system`

## Valid Event Examples

Create `data/valid_events.jsonl`:

```json
{"event_id":"evt_001","event_type":"page_view","user_id":"user_101","event_ts":"2026-06-30T14:00:00Z","source":"web","payload":{"page":"/home"}}
{"event_id":"evt_002","event_type":"video_play","user_id":"user_102","event_ts":"2026-06-30T14:01:00Z","source":"mobile","payload":{"video_id":"vid_900","position_seconds":0}}
{"event_id":"evt_003","event_type":"purchase","user_id":"user_103","event_ts":"2026-06-30T14:02:00Z","source":"web","payload":{"plan":"pro","amount":19.99}}
```

JSON Lines format means each line is one complete JSON object.
This format is convenient for Spark and Kafka examples because each event can be handled independently.

## Invalid Event Examples

Create `data/invalid_events.jsonl`:

```json
{"event_id":"","event_type":"page_view","user_id":"user_101","event_ts":"2026-06-30T14:00:00Z","source":"web","payload":{"page":"/home"}}
{"event_id":"evt_bad_002","event_type":"logout","user_id":"user_102","event_ts":"2026-06-30T14:01:00Z","source":"mobile","payload":{}}
{"event_id":"evt_bad_003","event_type":"purchase","user_id":"","event_ts":"not-a-timestamp","source":"browser","payload":{"amount":-10}}
```

## Validator Script

Create `validate_events.py`:

```python
import json
import sys
from datetime import datetime
from pathlib import Path

ALLOWED_EVENT_TYPES = {
    "page_view",
    "video_play",
    "video_pause",
    "add_to_cart",
    "purchase",
}

ALLOWED_SOURCES = {"web", "mobile", "api", "system"}
REQUIRED_FIELDS = {"event_id", "event_type", "user_id", "event_ts", "source", "payload"}


def is_timestamp(value):
    if not isinstance(value, str) or not value:
        return False

    normalized = value.replace("Z", "+00:00")

    try:
        datetime.fromisoformat(normalized)
        return True
    except ValueError:
        return False


def validate_event(event):
    errors = []
    missing = REQUIRED_FIELDS - set(event)

    if missing:
        errors.append(f"missing fields: {sorted(missing)}")

    if not event.get("event_id"):
        errors.append("event_id is required")

    if event.get("event_type") not in ALLOWED_EVENT_TYPES:
        errors.append("event_type is not allowed")

    if not event.get("user_id"):
        errors.append("user_id is required")

    if not is_timestamp(event.get("event_ts")):
        errors.append("event_ts must be an ISO-like timestamp")

    if event.get("source") not in ALLOWED_SOURCES:
        errors.append("source is not allowed")

    if not isinstance(event.get("payload"), dict):
        errors.append("payload must be an object")

    return errors


def main(path):
    input_path = Path(path)
    valid_count = 0
    invalid_count = 0

    for line_number, line in enumerate(input_path.read_text().splitlines(), start=1):
        if not line.strip():
            continue

        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            invalid_count += 1
            print(f"line {line_number}: invalid JSON: {exc}")
            continue

        errors = validate_event(event)

        if errors:
            invalid_count += 1
            print(f"line {line_number}: invalid event_id={event.get('event_id')}: {errors}")
        else:
            valid_count += 1
            print(f"line {line_number}: valid event_id={event.get('event_id')}")

    print(f"summary: valid={valid_count} invalid={invalid_count}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python validate_events.py data/valid_events.jsonl")

    main(sys.argv[1])
```

## Run the Validator

Validate the good file:

```bash
python validate_events.py data/valid_events.jsonl
```

Validate the bad file:

```bash
python validate_events.py data/invalid_events.jsonl
```

## Checkpoints

You are done when:

* Your schema defines fields, types, examples, and rules.
* The valid examples pass validation.
* The invalid examples fail with understandable error messages.
* You can explain why schema design helps Spark processing.

## Deliverables

Submit:

* `schema.md`.
* `data/valid_events.jsonl`.
* `data/invalid_events.jsonl`.
* `validate_events.py`.
* Terminal output from both validator runs.

## Reflection Questions

Answer briefly:

* Which fields should every event have?
* Which fields belong inside `payload` instead of at the top level?
* What could go wrong if every producer sends a different event shape?
