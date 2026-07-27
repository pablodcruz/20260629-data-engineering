"""Generate, validate, and summarize a small StreamFlow event dataset."""

from pathlib import Path

from airflow.decorators import dag, task
from pendulum import datetime

from pipeline_logic import (
    generate_events as generate_events_file,
    summarize_events as summarize_events_file,
    validate_events as validate_events_file,
)


DATA_DIR = Path("/opt/airflow/data")
EVENTS_PATH = DATA_DIR / "lab07_events.jsonl"
SUMMARY_PATH = DATA_DIR / "lab07_summary.json"


@dag(
    dag_id="lab07_basic_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
    tags=["streamflow", "lab"],
)
def lab07_basic_pipeline():
    """Define the three-task dependency chain for the teaching pipeline."""

    @task
    def generate_events() -> str:
        # Returning the path stores a small XCom pointer, not the event data.
        # Larger payloads belong in shared storage rather than Airflow metadata.
        return generate_events_file(EVENTS_PATH)

    @task
    def validate_events(events_path: str) -> str:
        # The upstream return value becomes this task's argument, which creates
        # an explicit generate_events >> validate_events dependency.
        return validate_events_file(Path(events_path))

    @task
    def summarize_events(events_path: str) -> str:
        return summarize_events_file(Path(events_path), SUMMARY_PATH)

    summarize_events(validate_events(generate_events()))


# Airflow discovers this module-level DAG object while scanning the dags folder.
lab07_basic_pipeline()
