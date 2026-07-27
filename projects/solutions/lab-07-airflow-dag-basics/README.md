# Lab 7 Solution - Airflow DAG Basics

Original lab: [`../../labs/lab-07-airflow-dag-basics.md`](../../labs/lab-07-airflow-dag-basics.md)

## Files

- `docker-compose.yml` runs a pinned Airflow standalone container.
- `dags/lab07_basic_pipeline.py` defines the three Airflow tasks and dependencies.
- `dags/pipeline_logic.py` contains independently testable file-processing logic.
- `tests/test_pipeline_logic.py` verifies generation, validation, and summary behavior.
- `data` and `logs` are bind-mounted runtime directories whose generated files
  are intentionally ignored by Git.

## 1. Check Docker and Configure the Host

From this solution directory, run:

```bash
docker compose version
docker compose config --quiet
```

If Homebrew installed Compose as a standalone plugin that Docker has not yet
discovered, use `docker-compose` in place of `docker compose` for every command.

macOS and Windows users can keep the container's default Airflow UID. On Linux,
record the host UID so bind-mounted logs are not owned by an unrelated user:

```bash
printf 'AIRFLOW_UID=%s\n' "$(id -u)" > .env
```

Port 8080 is the default. If it is already occupied, set a different host port
for the current Bash session before starting the service:

```bash
export AIRFLOW_PORT=8081
```

## 2. Run the Host-Side Automated Tests

The processing logic uses only Python's standard library, so Airflow does not
need to be installed on the host:

```bash
python3 -m unittest discover -s tests -v
```

Expected result:

```text
Ran 5 tests

OK
```

## 3. Start Airflow

```bash
docker compose up -d
docker compose ps
docker compose logs --tail=100 airflow
```

The first image download and initialization may take several minutes. Repeat
`docker compose ps` until the service reports `healthy` before continuing.

Check Airflow's view of the DAG:

```bash
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags list | grep lab07_basic_pipeline
docker compose exec airflow airflow tasks list lab07_basic_pipeline
```

Expected results:

- The import-error command reports no errors.
- The DAG list contains `lab07_basic_pipeline`.
- The task list contains `generate_events`, `validate_events`, and
  `summarize_events`.

## 4. Run a Synchronous DAG Test

Airflow's DAG test command executes the complete dependency chain and returns
only after all tasks finish:

```bash
docker compose exec airflow \
  airflow dags test lab07_basic_pipeline 2026-07-22
```

Expected task order:

```text
generate_events -> validate_events -> summarize_events
```

Expected generated summary:

```json
{
  "total_events": 3,
  "events_by_type": {
    "page_view": 1,
    "purchase": 1,
    "video_play": 1
  }
}
```

Inspect both artifacts from the host:

```bash
cat data/lab07_events.jsonl
cat data/lab07_summary.json
```

## 5. Trigger and Observe a Scheduled DAG Run

Trigger a regular run handled by the standalone scheduler:

```bash
docker compose exec airflow airflow dags trigger lab07_basic_pipeline
docker compose exec airflow airflow dags list-runs -d lab07_basic_pipeline
```

Repeat the list-runs command until the newest run reports `success`. If it
reports `failed`, inspect task logs and import errors:

```bash
docker compose logs --tail=200 airflow
docker compose exec airflow airflow dags list-import-errors
```

To use the web interface, read the generated password and open the configured
host port. The username is `admin`:

```bash
docker compose exec airflow \
  cat /opt/airflow/standalone_admin_password.txt
```

```text
http://localhost:8080
```

Use `http://localhost:8081` if `AIRFLOW_PORT=8081` was selected.

## Dependency Explanation

`generate_events` returns the event-file path through Airflow's XCom mechanism.
Passing that result to `validate_events` creates the first dependency. Passing
the validator's result to `summarize_events` creates the second. Airflow will
not schedule a downstream task until its upstream task succeeds, so invalid or
missing input stops the summary instead of producing misleading output. Only a
small path string travels through XCom; the event data remains in shared storage.

## Reflection Answers

### Why use Airflow instead of one Python script?

Airflow records each task's state, retries failed tasks, exposes task-specific
logs, and makes dependencies visible. A single script can call the same
functions, but it does not provide durable orchestration history or independent
task recovery without additional infrastructure.

### Is `airflow standalone` a production deployment?

No. It combines initialization, the scheduler, and webserver for convenient
local learning. Production deployments separate components, use an external
metadata database, protect credentials, and plan for availability and scaling.

## Runtime Evidence

Run the commands above and save real CLI output or screenshots here if a
submission requires them. Runtime evidence is intentionally not prefilled.

## Cleanup

Stop and remove the container and network, then remove only generated data:

```bash
docker compose down
rm -f data/lab07_events.jsonl data/lab07_summary.json
deactivate 2>/dev/null || true
```

Airflow logs remain ignored under `logs/` for debugging. Delete them only when
you no longer need that evidence.
