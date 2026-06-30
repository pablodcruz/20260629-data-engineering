# Lab 2 - Kafka Producer and Consumer

## Objective

Send event data into Kafka and read it back using command-line tools and Python.

## Scenario

Kafka is the event backbone for StreamFlow.
In this lab, you will start a local Kafka broker, create a topic, publish JSON event messages, and consume them back.

## What You Will Build

You will create:

* A one-broker Kafka environment using Docker Compose.
* A Kafka topic named `streamflow.events`.
* A Python producer that sends sample JSON events.
* A Python consumer that reads events from the topic.

## Prerequisites

* Docker is running.
* Python 3.10 or later is installed.
* Lab 1 Docker commands feel comfortable.

## Suggested Folder

From your lab workspace:

```bash
mkdir -p lab-02-kafka
cd lab-02-kafka
touch docker-compose.yml producer.py consumer.py
```

## Starter Docker Compose File

Create `docker-compose.yml`:

```yaml
services:
  kafka:
    image: bitnami/kafka:3.7
    container_name: streamflow_lab02_kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: broker,controller
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      ALLOW_PLAINTEXT_LISTENER: "yes"
```

Start Kafka:

```bash
docker compose up -d
docker compose logs kafka
```

Give Kafka 20-30 seconds to finish starting before creating topics.

## Create a Topic

Create the topic:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic streamflow.events --partitions 3 --replication-factor 1
```

List topics:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Describe the topic:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic streamflow.events
```

## Manual Producer and Consumer

Start a console consumer in one terminal:

```bash
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic streamflow.events --from-beginning
```

Start a console producer in a second terminal:

```bash
docker compose exec -it kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic streamflow.events
```

Paste these messages into the producer terminal one line at a time:

```json
{"event_id":"evt_001","event_type":"page_view","user_id":"user_101","event_ts":"2026-06-30T14:00:00Z","source":"web","payload":{"page":"/home"}}
{"event_id":"evt_002","event_type":"purchase","user_id":"user_102","event_ts":"2026-06-30T14:02:00Z","source":"mobile","payload":{"amount":29.99}}
{"event_id":"evt_003","event_type":"page_view","user_id":"user_103","event_ts":"2026-06-30T14:05:00Z","source":"web","payload":{"page":"/pricing"}}
```

Press `Ctrl+C` to stop the producer or consumer when finished.

## Python Producer

Create a virtual environment:

```bash
python -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
pip install kafka-python
```

Create `producer.py`:

```python
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

events = [
    {
        "event_id": "evt_101",
        "event_type": "page_view",
        "user_id": "user_201",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "web",
        "payload": {"page": "/home"},
    },
    {
        "event_id": "evt_102",
        "event_type": "add_to_cart",
        "user_id": "user_202",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "mobile",
        "payload": {"sku": "sku_001", "quantity": 2},
    },
    {
        "event_id": "evt_103",
        "event_type": "purchase",
        "user_id": "user_202",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "mobile",
        "payload": {"amount": 49.99},
    },
]

for event in events:
    producer.send("streamflow.events", event)
    print(f"sent {event['event_id']}")
    time.sleep(1)

producer.flush()
producer.close()
print("done")
```

Run it:

```bash
python producer.py
```

## Python Consumer

Create `consumer.py`:

```python
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "streamflow.events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="streamflow-lab02-consumer",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    consumer_timeout_ms=10000,
)

count = 0

for message in consumer:
    count += 1
    event = message.value
    print(
        f"topic={message.topic} partition={message.partition} "
        f"offset={message.offset} event_id={event.get('event_id')} "
        f"type={event.get('event_type')}"
    )

print(f"read {count} messages")
consumer.close()
```

Run it:

```bash
python consumer.py
```

## Checkpoints

You are done when:

* `streamflow.events` appears in the topic list.
* The console consumer displays messages sent by the console producer.
* `producer.py` sends at least three events.
* `consumer.py` reads and prints event IDs from Kafka.

## Deliverables

Submit:

* `docker-compose.yml`.
* `producer.py`.
* `consumer.py`.
* Terminal output showing messages were produced and consumed.
* A short explanation of what a Kafka topic is.

## Common Issues

| Problem | Likely Cause | Fix |
| ------- | ------------ | --- |
| `NoBrokersAvailable` | Kafka is still starting or not reachable | Wait 30 seconds and run `docker compose ps` |
| Topic already exists | The topic was created earlier | Continue or delete/recreate the stack |
| Consumer shows no messages | Consumer group already read them | Change `group_id` or use the console consumer with `--from-beginning` |
| Port `9092` already allocated | Another Kafka broker is running | Stop the other broker or map a different host port |

## Cleanup

When finished:

```bash
docker compose down
deactivate
```
