# Lab 8 - Airflow + Spark Submit

## Objective

Use Airflow to orchestrate a Spark batch job.

## Scenario

Spark does the data processing, but Airflow decides when the processing happens.
In this lab, Airflow runs a `spark-submit` command, waits for it to finish, and checks that output was created.

## What You Will Build

You will create:

* A custom Airflow container with Java and PySpark installed.
* A Spark job script.
* An Airflow DAG that runs `spark-submit`.
* A small validation task after the Spark job.

## Prerequisites

* Docker is running.
* Lab 5 and Lab 7 concepts are familiar.
* Port `8080` is available.

## Suggested Folder

From your lab workspace:

```bash
mkdir -p lab-08-airflow-spark/dags
mkdir -p lab-08-airflow-spark/jobs
mkdir -p lab-08-airflow-spark/data
mkdir -p lab-08-airflow-spark/output
mkdir -p lab-08-airflow-spark/logs
cd lab-08-airflow-spark
touch Dockerfile docker-compose.yml
touch data/events.jsonl jobs/process_events.py dags/lab08_airflow_spark_submit.py
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

RUN pip install --no-cache-dir pyspark==3.5.1
```

## Docker Compose File

Create `docker-compose.yml`:

```yaml
services:
  airflow:
    build: .
    container_name: streamflow_lab08_airflow_spark
    ports:
      - "8080:8080"
    environment:
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./jobs:/opt/airflow/jobs
      - ./data:/opt/airflow/data
      - ./output:/opt/airflow/output
      - ./logs:/opt/airflow/logs
    command: standalone
```

## Input Data

Create `data/events.jsonl`:

```json
{"event_id":"evt_001","event_type":"page_view","user_id":"user_101","event_ts":"2026-06-30T14:00:00Z","source":"web","payload":{"page":"/home"}}
{"event_id":"evt_002","event_type":"video_play","user_id":"user_102","event_ts":"2026-06-30T14:01:00Z","source":"mobile","payload":{"video_id":"vid_001"}}
{"event_id":"evt_003","event_type":"purchase","user_id":"user_103","event_ts":"2026-06-30T14:03:00Z","source":"web","payload":{"amount":"19.99"}}
{"event_id":"evt_004","event_type":"page_view","user_id":"user_101","event_ts":"2026-06-30T14:05:00Z","source":"web","payload":{"page":"/pricing"}}
```

## Spark Job

Create `jobs/process_events.py`:

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
        .appName("Lab08AirflowSparkSubmit")
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

    summary_df = (
        events_df
        .filter(F.col("event_time").isNotNull())
        .groupBy("event_type", "source")
        .agg(F.count("*").alias("event_count"))
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

Create `dags/lab08_airflow_spark_submit.py`:

```python
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from pendulum import datetime


SPARK_INPUT = "/opt/airflow/data/events.jsonl"
SPARK_OUTPUT = "/opt/airflow/output/event_summary"


@dag(
    dag_id="lab08_airflow_spark_submit",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["streamflow", "spark", "lab"],
)
def lab08_airflow_spark_submit():
    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command=(
            "spark-submit /opt/airflow/jobs/process_events.py "
            f"--input {SPARK_INPUT} "
            f"--output {SPARK_OUTPUT}"
        ),
    )

    @task
    def validate_output():
        output_path = Path(SPARK_OUTPUT)

        if not output_path.exists():
            raise FileNotFoundError(f"Missing output folder: {output_path}")

        part_files = list(output_path.glob("part-*"))

        if not part_files:
            raise FileNotFoundError(f"No Spark part files found in {output_path}")

        print(f"Found {len(part_files)} Spark output file(s)")
        print(part_files[0].read_text())

    run_spark >> validate_output()


lab08_airflow_spark_submit()
```

## Start Airflow

Build and start the container:

```bash
docker compose up --build -d
```

Confirm Spark is available inside the Airflow container:

```bash
docker compose exec airflow spark-submit --version
```

Get the Airflow password:

```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

Open the UI:

```text
http://localhost:8080
```

## Trigger the DAG

```bash
docker compose exec airflow airflow dags trigger lab08_airflow_spark_submit
```

Check the run:

```bash
docker compose exec airflow airflow dags list-runs -d lab08_airflow_spark_submit
```

Inspect output from your host machine:

```bash
ls output/event_summary
```

## Checkpoints

You are done when:

* The Airflow container builds successfully.
* `spark-submit --version` works inside the Airflow container.
* The DAG run succeeds.
* `output/event_summary` contains a Spark part file.

## Deliverables

Submit:

* `Dockerfile`.
* `docker-compose.yml`.
* `jobs/process_events.py`.
* `dags/lab08_airflow_spark_submit.py`.
* A screenshot or copied output showing a successful DAG run.
* A short explanation of why Airflow should orchestrate the job instead of doing the Spark transformation itself.

## Common Issues

| Problem | Likely Cause | Fix |
| ------- | ------------ | --- |
| Docker build fails during `apt-get` | Network or package repository issue | Retry the build and confirm internet access |
| `spark-submit` not found | Custom image did not build or old image is running | Run `docker compose down` then `docker compose up --build -d` |
| DAG import error | Syntax problem in the DAG file | Run `docker compose exec airflow airflow dags list-import-errors` |
| Output missing | Spark job failed | Check task logs for `run_spark_job` |

## Cleanup

When finished:

```bash
docker compose down
```
