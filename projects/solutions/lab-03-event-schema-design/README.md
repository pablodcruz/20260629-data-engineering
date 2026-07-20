# Lab 3 Solution - Event Schema Design

Original lab: [`../../labs/lab-03-event-schema-design.md`](../../labs/lab-03-event-schema-design.md)

## Files

- `schema.md` defines the shared fields and event-specific payload rules.
- `data/valid_events.jsonl` contains three events expected to pass.
- `data/invalid_events.jsonl` contains three events expected to fail.
- `validate_events.py` validates structure, values, payloads, and unique IDs.
- `tests/test_validate_events.py` checks validator behavior using `unittest`.

JSON does not support comments, so the `.jsonl` fixtures intentionally contain
data only. Explanations live in the schema, validator comments, and this guide.

## 1. Open the Solution Folder

```bash
cd "/c/Users/Pablo/Documents/20260629-data-engineering-master/20260629-data-engineering-master/projects/solutions/lab-03-event-schema-design"
```

This lab uses only the Python standard library, so no virtual environment or
package installation is required.

## 2. Validate the Good Events

```bash
python validate_events.py data/valid_events.jsonl
```

Expected summary:

```text
summary: valid=3 invalid=0
```

Bash's `$?` should be `0` because every event passed:

```bash
echo $?
```

## 3. Validate the Bad Events

```bash
python validate_events.py data/invalid_events.jsonl
```

Expected summary:

```text
summary: valid=0 invalid=3
```

The command intentionally returns exit code `1` because invalid data was found.
That does not mean the validator crashed; it means the validator did its job.

The three lines demonstrate:

1. An empty event ID.
2. An unsupported event type.
3. An empty user ID, invalid timestamp, unsupported source, and negative amount.

## 4. Run the Automated Tests

```bash
python -m unittest discover -s tests -v
```

Expected result:

```text
Ran 7 tests

OK
```

## How the Validator Works

The validator processes each JSON Lines record independently. It first parses
the JSON, checks common fields, then applies rules for the selected event type.
It retains event IDs while reading the file so it can also identify duplicates.
All errors for a record are reported together, helping producers fix multiple
problems in one pass.

## Reflection Answers

### Which fields should every event have?

Every event needs `event_id`, `event_type`, `user_id`, `event_ts`, `source`, and
`payload`. These fields identify the record, describe what happened and when,
associate it with a user and producer, and carry event-specific details.

### Which fields belong inside `payload`?

Fields that only make sense for a particular event belong inside `payload`.
Examples include `page`, `video_id`, `position_seconds`, `sku`, `quantity`,
`plan`, and `amount`. Keeping them out of the common envelope prevents every
event from having many irrelevant top-level fields.

### What if producers send different event shapes?

Consumers would need producer-specific parsing branches, Spark could infer
conflicting or overly broad types, fields could silently become null, and jobs
might fail after deployment. A shared schema catches those inconsistencies near
the producer, where they are easier to diagnose.

## Cleanup

This lab starts no services and creates no generated output, so no cleanup is
required.
