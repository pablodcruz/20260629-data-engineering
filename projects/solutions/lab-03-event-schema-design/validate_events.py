"""Validate StreamFlow events stored in a JSON Lines file.

JSON Lines uses one complete JSON value per line. Validating lines independently
makes it possible to report every bad record instead of rejecting the whole file
after the first problem.
"""

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


ALLOWED_EVENT_TYPES = frozenset(
    {"page_view", "video_play", "video_pause", "add_to_cart", "purchase"}
)
ALLOWED_SOURCES = frozenset({"web", "mobile", "api", "system"})
REQUIRED_FIELDS = frozenset(
    {"event_id", "event_type", "user_id", "event_ts", "source", "payload"}
)


@dataclass(frozen=True)
class ValidationResult:
    """The validation outcome for one non-blank input line."""

    line_number: int
    event_id: object
    errors: tuple[str, ...]

    @property
    def is_valid(self) -> bool:
        """Return True when validation found no errors."""
        return not self.errors


def is_non_empty_string(value: object) -> bool:
    """Return True for strings containing at least one non-space character."""
    return isinstance(value, str) and bool(value.strip())


def is_number(value: object) -> bool:
    """Accept integers and floats but reject booleans, which are integers in Python."""
    return type(value) in (int, float)


def is_timestamp(value: object) -> bool:
    """Check for an ISO 8601 timestamp that includes timezone information."""
    if not is_non_empty_string(value):
        return False

    # Python 3.10 does not parse the JSON-friendly Z suffix directly, so convert
    # only a final Z to the equivalent UTC offset before parsing.
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return False

    # A timestamp without an offset is ambiguous when systems run in different
    # timezones, so the StreamFlow contract requires timezone-aware values.
    return parsed.tzinfo is not None


def validate_payload(event_type: object, payload: dict[str, object]) -> list[str]:
    """Apply rules that depend on the event type."""
    errors: list[str] = []

    if event_type == "page_view":
        if not is_non_empty_string(payload.get("page")):
            errors.append("page_view payload.page must be a non-empty string")

    elif event_type in {"video_play", "video_pause"}:
        if not is_non_empty_string(payload.get("video_id")):
            errors.append(f"{event_type} payload.video_id must be a non-empty string")
        position = payload.get("position_seconds")
        if not is_number(position) or position < 0:
            errors.append(
                f"{event_type} payload.position_seconds must be a non-negative number"
            )

    elif event_type == "add_to_cart":
        if not is_non_empty_string(payload.get("sku")):
            errors.append("add_to_cart payload.sku must be a non-empty string")
        quantity = payload.get("quantity")
        if type(quantity) is not int or quantity <= 0:
            errors.append("add_to_cart payload.quantity must be a positive integer")

    elif event_type == "purchase":
        amount = payload.get("amount")
        if not is_number(amount) or amount <= 0:
            errors.append("purchase payload.amount must be a positive number")
        if "plan" in payload and not is_non_empty_string(payload["plan"]):
            errors.append("purchase payload.plan must be a non-empty string")

    return errors


def validate_event(event: object) -> list[str]:
    """Validate the common fields and payload of one decoded JSON value."""
    if not isinstance(event, dict):
        return ["event must be a JSON object"]

    errors: list[str] = []
    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        errors.append(f"missing fields: {sorted(missing)}")

    # Check a field only when it exists; the missing-fields message already
    # explains absent values and avoids reporting the same defect twice.
    if "event_id" in event and not is_non_empty_string(event["event_id"]):
        errors.append("event_id must be a non-empty string")

    if "event_type" in event and event["event_type"] not in ALLOWED_EVENT_TYPES:
        errors.append("event_type is not allowed")

    if "user_id" in event and not is_non_empty_string(event["user_id"]):
        errors.append("user_id must be a non-empty string")

    if "event_ts" in event and not is_timestamp(event["event_ts"]):
        errors.append("event_ts must be an ISO 8601 timestamp with a timezone")

    if "source" in event and event["source"] not in ALLOWED_SOURCES:
        errors.append("source is not allowed")

    payload = event.get("payload")
    if "payload" in event and not isinstance(payload, dict):
        errors.append("payload must be an object")
    elif isinstance(payload, dict) and event.get("event_type") in ALLOWED_EVENT_TYPES:
        errors.extend(validate_payload(event["event_type"], payload))

    return errors


def validate_lines(lines: Iterable[str]) -> list[ValidationResult]:
    """Decode and validate JSON Lines text, including event ID uniqueness."""
    results: list[ValidationResult] = []
    seen_event_ids: set[str] = set()

    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue

        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            results.append(
                ValidationResult(
                    line_number,
                    None,
                    (f"invalid JSON at column {exc.colno}: {exc.msg}",),
                )
            )
            continue

        errors = validate_event(event)
        event_id = event.get("event_id") if isinstance(event, dict) else None

        # Uniqueness is a file-level rule: an individual event cannot determine
        # whether another line already used the same identifier.
        if is_non_empty_string(event_id):
            if event_id in seen_event_ids:
                errors.append(f"duplicate event_id: {event_id}")
            else:
                seen_event_ids.add(event_id)

        results.append(ValidationResult(line_number, event_id, tuple(errors)))

    return results


def validate_file(path: Path) -> list[ValidationResult]:
    """Read a UTF-8 JSON Lines file and return all validation results."""
    with path.open(encoding="utf-8") as input_file:
        return validate_lines(input_file)


def parse_args() -> argparse.Namespace:
    """Read the input path from the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="Path to a .jsonl event file")
    return parser.parse_args()


def main() -> int:
    """Print readable results and return a shell-friendly exit code."""
    args = parse_args()

    try:
        results = validate_file(args.path)
    except OSError as exc:
        print(f"could not read {args.path}: {exc}")
        return 2

    for result in results:
        if result.is_valid:
            print(f"line {result.line_number}: valid event_id={result.event_id}")
        else:
            details = "; ".join(result.errors)
            print(
                f"line {result.line_number}: "
                f"invalid event_id={result.event_id}: {details}"
            )

    valid_count = sum(result.is_valid for result in results)
    invalid_count = len(results) - valid_count
    print(f"summary: valid={valid_count} invalid={invalid_count}")

    # Exit code 0 means every event passed. Code 1 means validation worked and
    # found bad data, which is useful in scripts and automated pipelines.
    return 0 if invalid_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
