# Lab 2 Solution - Kafka Producer and Consumer

Original lab: [`../../labs/lab-02-kafka-producer-consumer.md`](../../labs/lab-02-kafka-producer-consumer.md)

## Files

- `docker-compose.yml` runs a single Kafka 4.3.1 broker in KRaft mode using
  Apache Kafka's official image.
- `producer.py` publishes three JSON events and waits for broker acknowledgement.
- `consumer.py` reads JSON events and prints Kafka metadata.
- `requirements.txt` defines the Python client dependency.

## 1. Start Kafka

Open Git Bash in this solution folder and run:

```bash
docker compose config --quiet
docker compose up -d
docker compose ps
docker compose logs kafka
```

Wait until `docker compose ps` reports the Kafka container as healthy. On the
first run, Docker may need a few minutes to download the image.

## 2. Create and Inspect the Topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic streamflow.events --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic streamflow.events
```

Confirm that `streamflow.events` exists and has three partitions.

## 3. Create the Python Environment

```bash
python -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If activation is unavailable, the scripts also work without it by replacing
`python` below with `.venv/Scripts/python.exe`.

## 4. Test the Producer and Consumer

Publish the sample events without a delay:

```bash
python producer.py --delay-seconds 0
```

Expected producer result: three lines containing `evt_101`, `evt_102`, and
`evt_103`, followed by `sent 3 messages`. Each event line also shows the
partition and offset acknowledged by Kafka.

Consume with a new group name so the test is repeatable:

```bash
python consumer.py --group-id lab02-test-1 --timeout-ms 3000
```

Expected consumer result: the three Python-produced event IDs are present and
the final line reports at least `read 3 messages`. If manual events were already
sent to the topic, the count will be higher.

For another clean read from the beginning, change the group name:

```bash
python consumer.py --group-id lab02-test-2 --timeout-ms 3000
```

Kafka remembers offsets by consumer group. Reusing a group that already read
the topic may correctly produce `read 0 messages` when no new events exist.

## Optional Console Test

In terminal 1:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic streamflow.events --from-beginning
```

In terminal 2:

```bash
docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic streamflow.events
```

Paste one valid JSON object on a single line and confirm that terminal 1 prints
it. Press `Ctrl+C` in both terminals afterward.

## Deliverable Response

A Kafka topic is a named, durable stream of records. Producers append records
to the topic, and consumers read them at their own pace. A topic can be split
into partitions for parallelism and ordering within each partition. Consumer
groups track offsets so multiple consumers can share work without every member
processing every record.

## Runtime Evidence

Broker verification on July 20, 2026 confirmed that the container reached the
`healthy` state and `streamflow.events` was created with three partitions and a
replication factor of one. Save your real producer and consumer output here, or
take a screenshot, after running the Python commands above.

## Cleanup

```bash
docker compose down
deactivate
```

Use `docker compose down -v` only if you intentionally want to remove attached
volumes and reset persisted broker data.
