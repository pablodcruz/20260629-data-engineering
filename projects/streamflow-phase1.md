# Project Phase 1

## Project Title

**StreamFlow Phase 1 - Containerized Stream Processing Platform**

## Project Description

Design and implement a fully containerized, end-to-end streaming data platform using Apache Kafka or a Kafka-compatible broker, PySpark, Apache Airflow, and Docker.
The platform ingests real-time event streams, processes them with Spark Structured Streaming and Spark batch jobs, and orchestrates bounded workflows using Airflow DAGs within a local distributed environment.

The goal is not only to make the tools run, but to understand how event-driven data systems are structured: producers publish events, Spark consumes and processes those events incrementally, durable outputs make the stream usable for later analytics, and Airflow coordinates repeatable downstream jobs.

## Phase 1 Prototype

The first slice should be intentionally small:

1. Generate synthetic events for a chosen domain.
2. Publish events to a Kafka-compatible broker.
3. Consume the topic with Spark Structured Streaming.
4. Store raw events as Parquet files with checkpointing.
5. Use Airflow to run a Spark summary job over the stored stream output.

The local broker may use Apache Kafka or Redpanda.
Redpanda is acceptable because it speaks the Kafka protocol and keeps the Docker setup lighter for early development.

Airflow should not be responsible for running an infinite streaming loop.
Use Airflow for bounded jobs such as starting a producer, triggering a finite summary job, validating output files, or publishing run metadata.

## Functional Goals

The platform will:

* Generate or receive simulated event data.
* Publish event messages to a Kafka-compatible topic.
* Process incoming events using Spark Structured Streaming.
* Persist raw stream output to Parquet with checkpointing.
* Run Spark batch summary jobs over the persisted stream output.
* Separate valid records from invalid or rejected records.
* Orchestrate jobs with Apache Airflow DAGs.
* Run core services locally with Docker Compose.
* Produce logs and output files that show what each run processed.

## Core Functional Scope

As a **data engineer**, I want to:

1. Start the full local platform with Docker Compose.
2. Produce generated events into Kafka or Redpanda.
3. Define a consistent event schema.
4. Read event data from the topic with Spark Structured Streaming.
5. Transform and validate events with PySpark DataFrames.
6. Write raw and curated stream outputs to durable storage.
7. Capture rejected or invalid events with reasons.
8. Schedule or trigger bounded downstream jobs through Airflow.
9. Review logs and outputs to debug failed runs.
10. Document how another engineer can run the project.

## Objective / Tools Used

* Apache Kafka
* Redpanda, optional Kafka-compatible local broker
* PySpark DataFrames / SparkSQL
* Spark Structured Streaming
* Apache Airflow
* Docker / Docker Compose
* Python
* Distributed Data Processing Concepts
* Workflow Orchestration

## Weeks During Training

Weeks 6-8

## Project Type

Group Project

## Learning Outcomes

By the end of this project, associates should be able to:

* Explain the role of Kafka, Spark, Airflow, and Docker in a data platform.
* Build event-driven data pipelines using Kafka.
* Apply Spark DataFrame transformations to event data.
* Consume Kafka topics using Spark Structured Streaming.
* Run Spark streaming and batch jobs locally and through `spark-submit`.
* Orchestrate bounded workflows using Airflow DAGs.
* Understand micro-batch architecture patterns.
* Parameterize jobs using broker addresses, topics, checkpoint paths, input paths, output paths, and runtime configuration.
* Debug common container, connection, file path, and orchestration issues.

## System Architecture Overview

### Data Flow

```text
Event Generator / Producer
    |
    v
Kafka-compatible Topic: streamflow.events
    |
    v
Spark Structured Streaming Ingest Job
    |
    v
Raw Parquet Stream Output + Checkpoint Directory
    |
    v
Spark Batch Summary Job
    |
    v
Curated Summary Outputs + Airflow Logs
```

### Core Components

| Component | Responsibility |
| --------- | -------------- |
| **Producer** | Creates synthetic event payloads and sends them to a Kafka-compatible topic |
| **Kafka / Redpanda** | Stores event messages in a topic for downstream consumers |
| **Spark Streaming Ingest Job** | Reads topic data incrementally and writes raw Parquet output with checkpoints |
| **Spark Batch Summary Job** | Reads persisted stream output and produces analytics summaries |
| **Airflow DAG** | Coordinates bounded tasks and records execution history |
| **Docker Compose** | Runs the broker, Airflow, Spark runtime, producer, and supporting services locally |
| **Data Quality Logic** | Separates valid records from invalid or rejected records |
| **Logs and Outputs** | Provide evidence of what happened during each run |

## Event Data Contract

Each event should follow a predictable structure.

| Field | Type | Required | Example | Notes |
| ----- | ---- | -------- | ------- | ----- |
| `event_id` | string | yes | `evt_001` | Unique event identifier |
| `event_type` | string | yes | `purchase` | Category of event |
| `event_ts` | timestamp string | yes | `2026-06-30T14:30:00Z` | Time the event occurred |
| `source` | string | yes | `simulator` | Source system or producer name |
| `payload` | object | yes | `{"amount": 19.99}` | Domain-specific event fields |
| `entity_id` | string | no | `user_123` | User, device, account, session, or other entity tied to the event |

Example event:

```json
{"event_id":"evt_001","event_type":"purchase","event_ts":"2026-06-30T14:30:00Z","source":"simulator","entity_id":"user_123","payload":{"amount":19.99,"currency":"USD"}}
```

Teams may choose any coherent event domain, such as device telemetry, ecommerce behavior, media playback, support tickets, banking-style transactions, or application logs.
The required part is a consistent schema and a payload that supports meaningful validation and summaries.

## Expected Outputs

| Output | Purpose |
| ------ | ------- |
| Kafka-compatible topic | Stores incoming generated events |
| Raw Parquet stream output | Stores events consumed by Spark Structured Streaming |
| Checkpoint directory | Tracks streaming progress so the job can resume safely |
| Curated events dataset | Contains valid, standardized event records if implemented separately from raw output |
| Rejected events dataset | Contains invalid records and rejection reasons |
| Event summary dataset | Aggregates event counts, entities, or domain metrics by type, source, or time window |
| Airflow logs | Shows orchestration history and task-level failures |

## Recommended Folder Structure

```text
p1-streamflow-containerized-stream-processing/
  airflow/
    dags/
      streamflow_daily_summary.py
  docker/
    airflow.Dockerfile
    compose.yml
    producer.Dockerfile
  kafka/
  spark/
    jobs/
      streaming_ingest.py
      daily_summary.py
  src/
    streamflow/
      __init__.py
      producer.py
      schemas.py
      quality.py
  data/
    raw/
    curated/
    rejects/
    checkpoints/
  tests/
  README.md
```

Prototype-compatible structure:

```text
p1-streamflow-containerized-stream-processing/
  airflow/dags/streamflow_daily_summary.py
  docker/airflow.Dockerfile
  docker/compose.yml
  docker/producer.Dockerfile
  spark/jobs/daily_summary.py
  spark/jobs/streaming_ingest.py
  src/streamflow/
  tests/
```

Teams may use a flatter structure if the README clearly explains where the producer, streaming ingest job, Airflow DAG, Docker files, and outputs live.

## Minimum Required Jobs

```text
src/streamflow/producer.py
```

Generates synthetic events for the chosen domain and publishes them to the configured topic.

```text
spark/jobs/streaming_ingest.py
```

Runs Spark Structured Streaming against the Kafka-compatible topic and writes raw Parquet output with a checkpoint path.

```text
spark/jobs/daily_summary.py
```

Runs as a bounded Spark batch job over the persisted Parquet output and writes a summary dataset.

```text
airflow/dags/streamflow_daily_summary.py
```

Triggers or coordinates the bounded summary workflow and validates that expected outputs exist.

Optional helper modules may hold schema definitions, validation rules, producer configuration, or shared logging.

## Previous Simple Structure

This older structure is still useful for learning the individual tools, but the final project should include an explicit streaming ingest job:

```text
streamflow/
  dags/
    streamflow_pipeline.py
  jobs/
    process_events.py
    quality_checks.py
  producers/
    event_producer.py
  config/
    pipeline.yml
  data/
    input/
    output/
    rejects/
  tests/
    test_event_validation.py
    test_transformations.py
  docker-compose.yml
  requirements.txt
  README.md
```

## Example Configuration

Configuration should control environment-specific values instead of hardcoding them throughout the code.

```yaml
kafka:
  bootstrap_servers: redpanda:9092
  topic: streamflow.events

spark:
  app_name: StreamFlowPhase1
  raw_output_path: /opt/streamflow/data/raw/events
  checkpoint_path: /opt/streamflow/data/checkpoints/events
  summary_output_path: /opt/streamflow/data/curated/daily_summary
  rejects_path: /opt/streamflow/data/rejects/events

pipeline:
  run_id: local_001
  allowed_sources:
    - simulator
    - web
    - mobile
    - api
  allowed_event_types:
    - page_view
    - add_to_cart
    - purchase
    - video_play
```

## Processing Strategy

The Phase 1 pipeline should use a simple, repeatable streaming pattern:

1. Generate or receive events.
2. Publish events to a Kafka-compatible topic.
3. Run a Spark Structured Streaming ingest job that reads from the topic.
4. Parse and validate event JSON using a defined schema.
5. Write raw events to Parquet with a checkpoint directory.
6. Write invalid records to a rejected output path when implemented.
7. Run a bounded Spark batch summary job over the persisted stream output.
8. Use Airflow to coordinate the summary job, output validation, and run metadata.

The streaming job may run continuously during a demo or use a finite trigger such as `availableNow` or `once` for local development.
The important requirement is that Spark consumes from the topic incrementally and uses checkpointing, not that the team operates a long-running production service.

Jobs should be safe to rerun during development.
Use clear output paths, run IDs, checkpoint paths, or overwrite behavior intentionally so teams understand what happens on each run.

## Validation and Data Quality Rules

| Check | Rule |
| ----- | ---- |
| Required fields | `event_id`, `event_type`, `event_ts`, `source`, and `payload` must exist |
| Timestamp parsing | `event_ts` must be parseable as a timestamp |
| Duplicate events | Duplicate `event_id` values should be detected |
| Allowed values | `event_type` and `source` should match known values |
| Rejected records | Invalid records should be written with a reason |
| Empty outputs | The pipeline should fail or warn if no records are produced |
| Checkpointing | Streaming ingestion should use a checkpoint location |

## Logging and Observability

Each run should produce clear logs that show:

* When the run started and ended.
* Which broker, topic, checkpoint path, input path, and output path were used.
* How many records were read.
* How many records passed validation.
* How many records were rejected.
* Where outputs were written.

Example log messages:

```text
INFO streamflow.producer.start topic=streamflow.events broker=redpanda:9092
INFO streamflow.ingest.start topic=streamflow.events checkpoint=/opt/streamflow/data/checkpoints/events
INFO streamflow.ingest.batch batch_id=12 input_rows=120 valid=116 rejected=4
INFO streamflow.summary.write output=/opt/streamflow/data/curated/daily_summary
INFO streamflow.end status=success
```

## Testing Strategy

| Test Type | Focus |
| --------- | ----- |
| Unit tests | Event validation, timestamp parsing, reject reason logic |
| Spark transformation tests | Input rows produce expected parsed, validated, and summary rows |
| Streaming smoke tests | Spark can read generated topic events and write Parquet output |
| Integration tests | Docker services start and Kafka-compatible topic can receive messages |
| Airflow smoke tests | DAG imports successfully and can be triggered |
| Manual demo checks | Final outputs exist and logs show successful execution |

Teams do not need a large test suite, but they should include enough checks to prove the core pipeline works.

## Non-Functional Expectations

* Code should be simple, modular, and readable.
* Runtime values should be configurable.
* Secrets and credentials should not be committed to Git.
* Docker services should have clear names and port mappings.
* Output folders should be organized and easy to inspect.
* The project README should explain setup, run commands, and troubleshooting tips.
* Each team member should make meaningful contributions through Git.

## Definition of Done

Phase 1 is complete when:

* Docker Compose starts the required local services.
* Kafka or Redpanda has a working event topic.
* A producer can send generated events.
* A Spark Structured Streaming job can consume topic events and write raw Parquet output.
* The streaming job uses a checkpoint path.
* A Spark batch job can read the persisted stream output and write a summary dataset.
* Invalid records are rejected or documented with reasons.
* Airflow can trigger or orchestrate bounded summary, validation, or reporting tasks.
* Logs show record counts and run status.
* The repository includes clear setup and run instructions.
* The team can demo the pipeline from start to finish.

## Stretch Goals

| Area | Optional Enhancement |
| ---- | -------------------- |
| Kafka | Add multiple topics for different event types |
| Spark | Add windowed streaming aggregations in addition to raw ingest |
| Storage | Add MinIO or S3-style object storage |
| Data Quality | Add Great Expectations or a custom validation report |
| Monitoring | Add a simple run metrics summary |
| Airflow | Add retries, task-level alerts, and parameterized DAG runs |
| Schema | Add schema versioning to event payloads |

## Supporting Labs

See [StreamFlow Phase 1 Labs](labs/README.md) for the suggested lab sequence that prepares associates for this project.

---
