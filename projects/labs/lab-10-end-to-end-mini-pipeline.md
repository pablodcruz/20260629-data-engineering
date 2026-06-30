# Lab 10 - End-to-End Mini Pipeline

## Objective

Connect the core StreamFlow tools into a smaller version of the final project.

## Scenario

The final project combines Docker, Kafka, Spark, and Airflow.
This lab builds a mini version of that platform:

1. Airflow generates sample events.
2. The events are sent to Kafka.
3. The same events are staged as a JSON Lines file.
4. Airflow runs a Spark job with `spark-submit`.
5. Spark writes a curated summary output.
6. Airflow validates that the output exists.

The file staging step keeps the Spark portion simple and reliable while still giving you Kafka practice.

## What You Will Build

You will create:

* A Docker Compose stack with Kafka and Airflow.
* A custom Airflow image with PySpark and Kafka Python client support.
* An Airflow DAG that generates, publishes, processes, and validates event data.
* A Spark job that produces an event summary.

## Prerequisites

* Docker is running.
* Labs 2, 5, 7, and 8 are familiar.
* Ports `8080` and `9092` are available.

## Suggested Folder

From your lab workspace:

```bash
mkdir -p lab-10-mini-pipeline/dags
mkdir -p lab-10-mini-pipeline/jobs
mkdir -p lab-10-mini-pipeline/landing
mkdir -p lab-10-mini-pipeline/output
mkdir -p lab-10-mini-pipeline/logs
cd lab-10-mini-pipeline
touch Dockerfile docker-compose.yml
touch jobs/process_landing_events.py dags/lab10_end_to_end_mini_pipeline.py
```

## Dockerfile

Create `Dockerfile`:

```dockerfile
FROM apache/airflow:2.9.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir pyspark==3.5.1 kafka-python==2.0.2
```

## Docker Compose File

Create `docker-compose.yml`:

```yaml
services:
  kafka:
    image: bitnami/kafka:3.7
    container_name: streamflow_lab10_kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: broker,controller
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_LISTENERS: INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_INTER_BROKER_LISTENER_NAME: INTERNAL
      ALLOW_PLAINTEXT_LISTENER: "yes"

  airflow:
    build: .
    container_name: streamflow_lab10_airflow
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./jobs:/opt/airflow/jobs
      - ./landing:/opt/airflow/landing
      - ./output:/opt/airflow/output
      - ./logs:/opt/airflow/logs
    command: standalone
```

## Spark Job

Create `jobs/process_landing_events.py`:

```python
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Lab10EndToEndMiniPipeline")
        .getOrCreate()
    )

    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), True),
            T.StructField("event_type", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("event_ts", T.StringType(), True),
            T.StructField("source", T.StringType(), True),
            T.StructField("payload", T.MapType(T.StringType(), T.StringType()), True),
        ]
    )

    events_df = (
        spark.read
        .schema(schema)
        .json(args.input)
        .withColumn("event_time", F.to_timestamp("event_ts"))
    )

    curated_df = events_df.filter(
        F.col("event_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("event_time").isNotNull()
    )

    summary_df = (
        curated_df
        .groupBy("event_type", "source")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .orderBy("event_type", "source")
    )

    summary_df.show(truncate=False)

    (
        summary_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(args.output)
    )

    spark.stop()


if __name__ == "__main__":
    main()
```

## Airflow DAG

Create `dags/lab10_end_to_end_mini_pipeline.py`:

```python
import json
from datetime import datetime, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from kafka import KafkaProducer
from pendulum import datetime as airflow_datetime


TOPIC = "streamflow.events"
KAFKA_BOOTSTRAP = "kafka:29092"
LANDING_PATH = "/opt/airflow/landing/events.jsonl"
SPARK_OUTPUT = "/opt/airflow/output/event_summary"


def build_events():
    now = datetime.now(timezone.utc).isoformat()

    return [
        {
            "event_id": "evt_001",
            "event_type": "page_view",
            "user_id": "user_101",
            "event_ts": now,
            "source": "web",
            "payload": {"page": "/home"},
        },
        {
            "event_id": "evt_002",
            "event_type": "video_play",
            "user_id": "user_102",
            "event_ts": now,
            "source": "mobile",
            "payload": {"video_id": "vid_001"},
        },
        {
            "event_id": "evt_003",
            "event_type": "purchase",
            "user_id": "user_103",
            "event_ts": now,
            "source": "web",
            "payload": {"amount": "19.99"},
        },
        {
            "event_id": "evt_004",
            "event_type": "page_view",
            "user_id": "user_101",
            "event_ts": now,
            "source": "web",
            "payload": {"page": "/pricing"},
        },
    ]


@dag(
    dag_id="lab10_end_to_end_mini_pipeline",
    start_date=airflow_datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["streamflow", "mini-pipeline", "lab"],
)
def lab10_end_to_end_mini_pipeline():
    @task
    def generate_and_publish_events():
        events = build_events()
        landing_path = Path(LANDING_PATH)
        landing_path.parent.mkdir(parents=True, exist_ok=True)

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

        with landing_path.open("w", encoding="utf-8") as handle:
            for event in events:
                producer.send(TOPIC, event)
                handle.write(json.dumps(event) + "\n")
                print(f"published and staged {event['event_id']}")

        producer.flush()
        producer.close()
        return LANDING_PATH

    run_spark = BashOperator(
        task_id="run_spark_summary",
        bash_command=(
            "spark-submit /opt/airflow/jobs/process_landing_events.py "
            f"--input {LANDING_PATH} "
            f"--output {SPARK_OUTPUT}"
        ),
    )

    @task
    def validate_output():
        output_path = Path(SPARK_OUTPUT)
        part_files = list(output_path.glob("part-*"))

        if not part_files:
            raise FileNotFoundError(f"No Spark output files found in {output_path}")

        print("Spark output preview:")
        print(part_files[0].read_text())

    generate_and_publish_events() >> run_spark >> validate_output()


lab10_end_to_end_mini_pipeline()
```

## Start the Stack

Build and start containers:

```bash
docker compose up --build -d
```

Check services:

```bash
docker compose ps
```

Create the Kafka topic:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:29092 --create --topic streamflow.events --partitions 3 --replication-factor 1
```

List topics:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:29092 --list
```

## Trigger the Pipeline

Wait until Airflow is fully started, then trigger the DAG:

```bash
docker compose exec airflow airflow dags trigger lab10_end_to_end_mini_pipeline
```

Check DAG runs:

```bash
docker compose exec airflow airflow dags list-runs -d lab10_end_to_end_mini_pipeline
```

Inspect staged data:

```bash
cat landing/events.jsonl
```

Inspect Spark output:

```bash
ls output/event_summary
```

Consume Kafka messages:

```bash
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic streamflow.events --from-beginning --timeout-ms 10000
```

## Checkpoints

You are done when:

* Kafka and Airflow are both running.
* The `streamflow.events` topic exists.
* The Airflow DAG run succeeds.
* `landing/events.jsonl` contains generated events.
* `output/event_summary` contains Spark output.
* The Kafka consumer can read the published events.

## Deliverables

Submit:

* `Dockerfile`.
* `docker-compose.yml`.
* `jobs/process_landing_events.py`.
* `dags/lab10_end_to_end_mini_pipeline.py`.
* Output from the Kafka consumer.
* Output folder from Spark.
* A short demo plan explaining the pipeline flow.

## Common Issues

| Problem | Likely Cause | Fix |
| ------- | ------------ | --- |
| DAG fails in publish step | Kafka is not ready or topic missing | Create the topic and retry the DAG |
| `NoBrokersAvailable` | Airflow cannot reach Kafka | Confirm `KAFKA_BOOTSTRAP` is `kafka:29092` inside Docker |
| Spark output missing | Spark task failed | Inspect Airflow task logs for `run_spark_summary` |
| Host cannot use Kafka on `localhost:9092` | Listener configuration mismatch | Use the provided Compose file exactly |
| Port conflict | Another Airflow or Kafka stack is running | Stop the old stack or change host ports |

## Cleanup

When finished:

```bash
docker compose down
```
