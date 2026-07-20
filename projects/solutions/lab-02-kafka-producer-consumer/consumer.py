"""Consume StreamFlow events from Kafka and print their metadata.

Kafka stores values as bytes. This consumer converts each JSON byte string back
to a Python dictionary and shows where Kafka stored the record.
"""

import argparse
import json

from kafka import KafkaConsumer


def parse_args() -> argparse.Namespace:
    """Read optional broker, topic, group, and timeout settings."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="streamflow.events")
    parser.add_argument("--group-id", default="streamflow-lab02-consumer")
    parser.add_argument("--timeout-ms", type=int, default=10_000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        # A brand-new consumer group starts at the earliest available record.
        # An existing group resumes from its previously committed offset.
        auto_offset_reset="earliest",
        # Auto-commit periodically records this group's progress in Kafka.
        enable_auto_commit=True,
        group_id=args.group_id,
        # Convert Kafka's raw bytes back into the dictionary used below.
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        # End this learning script after the topic stays idle for this long.
        # Production consumers typically remain alive and wait indefinitely.
        consumer_timeout_ms=args.timeout_ms,
    )

    count = 0
    try:
        for message in consumer:
            count += 1
            event = message.value
            # Topic, partition, and offset uniquely identify a Kafka record.
            print(
                f"topic={message.topic} partition={message.partition} "
                f"offset={message.offset} event_id={event.get('event_id')} "
                f"type={event.get('event_type')}"
            )
    finally:
        # Closing releases the network connection and consumer-group membership.
        consumer.close()

    print(f"read {count} messages")


if __name__ == "__main__":
    main()
