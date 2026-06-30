# Lab 11 - Debugging Distributed Systems

## Objective

Practice diagnosing common problems in Docker, Kafka, Spark, and Airflow environments.

## Scenario

Distributed systems fail in layers.
A Spark error might actually be a file path issue.
An Airflow failure might be a DAG import error.
A Kafka connection error might be a listener configuration problem.
This lab gives you a repeatable debugging process.

## What You Will Build

You will create a debugging report using commands, symptoms, root causes, and fixes.
You do not need to write a new pipeline for this lab.
Use one of the previous lab stacks, preferably Lab 10.

## Prerequisites

* One previous Docker-based lab stack available.
* Comfort reading terminal output.

## Debugging Mindset

Use this order:

1. Confirm the service is running.
2. Read the logs.
3. Confirm ports and paths.
4. Reproduce the error.
5. Change one thing.
6. Run again.
7. Record the symptom, cause, and fix.

## Command Reference

Run from the folder containing the relevant `docker-compose.yml`.

### Docker

```bash
docker compose ps
docker compose logs
docker compose logs --tail=100 airflow
docker compose logs --tail=100 kafka
docker compose restart airflow
docker compose down
docker compose up -d
```

### Kafka

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:29092 --list
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic streamflow.events
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic streamflow.events --from-beginning --timeout-ms 10000
```

If you are using the Lab 2 Kafka stack, use `localhost:9092` inside the Kafka container:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Airflow

```bash
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags list-runs -d lab10_end_to_end_mini_pipeline
docker compose exec airflow airflow tasks list lab10_end_to_end_mini_pipeline
```

### Spark

```bash
docker compose exec airflow spark-submit --version
docker compose exec airflow ls /opt/airflow/jobs
docker compose exec airflow ls /opt/airflow/landing
docker compose exec airflow ls /opt/airflow/output
```

## Scenario 1 - Port Conflict

### Symptom

Docker fails with a message like:

```text
port is already allocated
```

### Diagnose

Check which ports your Compose file uses:

```bash
docker compose config
```

Look for mappings like:

```yaml
ports:
  - "8080:8080"
```

### Fix

Change the host port:

```yaml
ports:
  - "8081:8080"
```

Then restart:

```bash
docker compose down
docker compose up -d
```

Open Airflow at `http://localhost:8081`.

## Scenario 2 - Airflow DAG Does Not Appear

### Symptom

The DAG file exists, but it does not show up in the Airflow UI.

### Diagnose

```bash
docker compose exec airflow airflow dags list-import-errors
```

### Common Causes

* Python syntax error.
* Missing import.
* DAG file saved in the wrong folder.
* File name does not end in `.py`.

### Fix

Correct the Python error, then wait for the scheduler to parse the file again.
You can also restart Airflow:

```bash
docker compose restart airflow
```

## Scenario 3 - Kafka Connection Fails

### Symptom

Python shows:

```text
NoBrokersAvailable
```

### Diagnose

Check Kafka container health:

```bash
docker compose ps
docker compose logs --tail=100 kafka
```

List topics from inside the Docker network:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:29092 --list
```

### Common Causes

* Kafka is still starting.
* The topic was not created.
* The code is using `localhost:9092` from inside another container.

### Fix

Inside Docker, services should usually connect to Kafka with the service name:

```python
bootstrap_servers = "kafka:29092"
```

From your host machine, use:

```python
bootstrap_servers = "localhost:9092"
```

## Scenario 4 - Spark Cannot Find Input Files

### Symptom

Spark fails with a path error, such as:

```text
Path does not exist
```

### Diagnose

Check whether the file exists inside the container:

```bash
docker compose exec airflow ls /opt/airflow/landing
docker compose exec airflow cat /opt/airflow/landing/events.jsonl
```

### Common Causes

* The host folder was not mounted into the container.
* The Spark job uses a host path instead of a container path.
* The upstream task did not create the file.

### Fix

Use container paths inside Airflow and Spark:

```text
/opt/airflow/landing/events.jsonl
/opt/airflow/output/event_summary
```

Confirm the Compose file includes matching volume mounts:

```yaml
volumes:
  - ./landing:/opt/airflow/landing
  - ./output:/opt/airflow/output
```

## Scenario 5 - Docker Image Change Does Not Show Up

### Symptom

You changed the `Dockerfile`, but the container still behaves the same.

### Diagnose

Check whether the image was rebuilt:

```bash
docker compose build
```

### Fix

Rebuild and recreate the container:

```bash
docker compose down
docker compose up --build -d
```

## Debugging Report Template

Create `debugging-report.md`:

```markdown
# Debugging Report

## Issue 1

Symptom:

Command that showed the problem:

Relevant log line:

Root cause:

Fix:

How I verified the fix:

## Issue 2

Symptom:

Command that showed the problem:

Relevant log line:

Root cause:

Fix:

How I verified the fix:
```

## Checkpoints

You are done when:

* You used at least five diagnostic commands.
* You documented at least two issues.
* Each issue includes symptom, root cause, fix, and verification.
* You can explain whether the issue was Docker, Kafka, Spark, Airflow, or file-path related.

## Deliverables

Submit:

* `debugging-report.md`.
* The commands you ran.
* Relevant log snippets.
* A short reflection on which debugging command was most useful.

## Reflection Questions

Answer briefly:

* Why should you check container logs before changing code?
* What is the difference between a host path and a container path?
* Why does `localhost` mean different things depending on where code runs?
