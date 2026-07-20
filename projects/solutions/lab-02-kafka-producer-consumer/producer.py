"""Publish sample StreamFlow events to Kafka.

The producer converts Python dictionaries to JSON bytes, sends them to a Kafka
topic, and waits for the broker to acknowledge each record.
"""

import argparse
import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


def parse_args() -> argparse.Namespace:
    """Read optional connection and pacing settings from the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="streamflow.events")
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=1.0,
        help="Pause between events; use 0 for a fast test.",
    )
    return parser.parse_args()


def sample_events() -> list[dict[str, object]]:
    """Return three example events that follow the StreamFlow event shape."""
    # Use an explicit UTC timestamp so downstream systems do not need to guess
    # which timezone produced the event.
    timestamp = datetime.now(timezone.utc).isoformat()
    return [
        {
            "event_id": "evt_101",
            "event_type": "page_view",
            "user_id": "user_201",
            "event_ts": timestamp,
            "source": "web",
            "payload": {"page": "/home"},
        },
        {
            "event_id": "evt_102",
            "event_type": "add_to_cart",
            "user_id": "user_202",
            "event_ts": timestamp,
            "source": "mobile",
            "payload": {"sku": "sku_001", "quantity": 2},
        },
        {
            "event_id": "evt_103",
            "event_type": "purchase",
            "user_id": "user_202",
            "event_ts": timestamp,
            "source": "mobile",
            "payload": {"amount": 49.99},
        },
    ]


def main() -> None:
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        # Kafka transports bytes. The serializer performs the same JSON-to-byte
        # conversion for every dictionary passed to producer.send().
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        # "all" asks every in-sync replica to acknowledge the write. This lab
        # has one replica, but the setting demonstrates a durable-send pattern.
        acks="all",
    )

    try:
        for event in sample_events():
            # send() is normally asynchronous. Calling get() waits for Kafka's
            # response and exposes the partition and offset assigned to it.
            metadata = producer.send(args.topic, value=event).get(timeout=10)
            print(
                f"sent event_id={event['event_id']} "
                f"partition={metadata.partition} offset={metadata.offset}"
            )
            time.sleep(args.delay_seconds)
        # Ensure buffered records leave the client before the program exits.
        producer.flush(timeout=10)
    finally:
        # finally runs even when a send fails, preventing a leaked connection.
        producer.close(timeout=10)

    print("sent 3 messages")


if __name__ == "__main__":
    main()
