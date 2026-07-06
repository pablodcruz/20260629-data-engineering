# Git Merging Strategy for StreamFlow Phase 1

## Purpose

This guide is for a team of 3-4 people building **StreamFlow Phase 1 - Containerized Stream Processing Platform**.
The goal is to let everyone work in parallel without breaking the shared `main` branch.

Phase 1 has several moving parts:

* Event producer
* Kafka or Redpanda broker setup
* Spark Structured Streaming ingest job
* Spark summary job
* Airflow DAG
* Docker Compose and Dockerfiles
* README, runbook, tests, and demo notes

Because these parts depend on each other, the team should merge in small steps and verify the project after each merge.

## Team Roles

For a team of 3:

| Person | Primary Ownership |
| ------ | ----------------- |
| Teammate 1 | Producer, event schema, sample events |
| Teammate 2 | Spark Structured Streaming ingest and validation |
| Teammate 3 | Docker Compose, Airflow DAG, README, demo flow |

For a team of 4:

| Person | Primary Ownership |
| ------ | ----------------- |
| Teammate 1 | Producer, event schema, sample events |
| Teammate 2 | Spark Structured Streaming ingest and validation |
| Teammate 3 | Spark summary job, data quality checks, tests |
| Teammate 4 | Docker Compose, Airflow DAG, README, demo flow |

Ownership does not mean only one person can touch a file.
It means one person is responsible for reviewing changes in that area and keeping the team aligned.

## Branch Rules

Keep `main` stable.
Do not commit directly to `main` unless the team explicitly agrees it is a tiny documentation fix.

Use one branch per task:

```bash
git switch -c feature/event-producer
git switch -c feature/streaming-ingest
git switch -c feature/airflow-summary-dag
git switch -c fix/docker-redpanda-listeners
git switch -c docs/update-runbook
```

Recommended branch prefixes:

| Prefix | Use For |
| ------ | ------- |
| `feature/` | New project functionality |
| `fix/` | Bug fixes |
| `docs/` | README, runbook, architecture notes |
| `test/` | Test files or test-only changes |
| `chore/` | Cleanup that does not change behavior |

## Daily Workflow

Start each work session from the latest `main`:

```bash
git switch main
git pull origin main
git switch -c feature/your-task-name
```

While working, check what changed:

```bash
git status
git diff
```

Commit small, working pieces:

```bash
git add path/to/file
git commit -m "Add event producer config"
```

Before opening a pull request:

```bash
git fetch origin
git pull --rebase origin main
```

If the rebase creates conflicts, resolve them locally, run the relevant checks, then push:

```bash
git push -u origin feature/your-task-name
```

If you already pushed the branch before rebasing:

```bash
git push --force-with-lease
```

Use `--force-with-lease`, not plain `--force`.
It protects you from overwriting a teammate's newer remote changes by accident.

## Pull Request Rules

Every pull request should be small enough for a teammate to review in 10-15 minutes.

A good PR usually changes one area:

* Producer only
* Streaming ingest only
* Docker Compose only
* Airflow DAG only
* README/runbook only
* Tests for one module

Avoid PRs that change producer, Docker, Spark, Airflow, and README all at once unless it is the final integration PR.

Each PR should include:

* What changed
* How to run or test it
* Any files or services affected
* Any known issue or follow-up task

Example PR description:

```markdown
## What Changed

Adds `src/streamflow/producer.py` to generate sample ecommerce events and publish them to `streamflow.events`.

## How I Tested

- Ran `python -m streamflow.producer --count 10`
- Confirmed events appeared in Redpanda console consumer

## Notes

Requires the broker service from `docker/compose.yml`.
```

## Review Rules

At least one teammate should review each PR before merge.
For risky files, ask the owner to review.

Risky shared files include:

* `docker/compose.yml`
* Dockerfiles
* shared config files
* event schema files
* `README.md`
* Airflow DAGs
* Spark job entrypoints

Reviewers should check:

* Does the change match the Phase 1 architecture?
* Does it avoid committing generated data, logs, secrets, or local environment files?
* Can another teammate run it from the README or PR notes?
* Does it use the agreed topic, paths, and config names?
* Does it keep `main` runnable?

## Merge Strategy

Use **squash merge** for most PRs.
This keeps `main` readable and avoids a noisy history from many small student commits.

Use a normal merge commit only for a large integration branch when preserving branch history helps explain the work.

Do not use rebase directly on `main`.
Rebase your feature branch onto `main`, then merge through a PR.

Preferred flow:

```text
feature branch
    |
    v
pull request
    |
    v
review
    |
    v
squash merge into main
    |
    v
everyone pulls latest main
```

After a teammate merges:

```bash
git switch main
git pull origin main
```

Then either create a new branch or update your current branch:

```bash
git switch feature/your-task-name
git pull --rebase origin main
```

## Recommended Integration Order

The team should avoid waiting until the last day to connect everything.
Merge a small vertical slice early.

Suggested order:

1. Merge base project structure and README.
2. Merge Docker Compose with Kafka or Redpanda running.
3. Merge event schema and producer.
4. Merge Spark Structured Streaming ingest that writes raw Parquet with checkpoints.
5. Merge Spark summary job that reads the raw Parquet output.
6. Merge Airflow DAG that runs the bounded summary workflow.
7. Merge validation, tests, and troubleshooting notes.
8. Merge final demo script and screenshots or sample outputs.

The first integration target should be small:

```text
producer -> broker topic -> streaming_ingest.py -> raw Parquet output
```

Once that works, the Airflow and summary pieces become easier to reason about.

## Conflict Hotspots

These files are likely to cause merge conflicts:

| File | Why It Conflicts | Strategy |
| ---- | ---------------- | -------- |
| `README.md` | Everyone edits setup and run instructions | Assign one README owner and ask others to propose changes in PR notes |
| `docker/compose.yml` | Multiple services and ports live in one file | Make small compose changes and request review from Docker owner |
| `config/*.yml` | Topic names, paths, and service names are shared | Agree on names before coding |
| `src/streamflow/schemas.py` | Producer and Spark jobs both depend on schema | Treat schema changes as team decisions |
| `airflow/dags/*.py` | DAG paths must match Docker volumes and Spark jobs | Merge after Docker paths are stable |

When a conflict happens:

1. Stop and read both versions.
2. Keep the behavior that matches the current architecture.
3. Ask the file owner if the correct answer is unclear.
4. Run the relevant command after resolving.
5. Commit the conflict resolution with a clear message.

## Files Not To Commit

Do not commit:

* `.env`
* secrets or credentials
* local virtual environments
* Python cache folders
* Airflow logs
* Spark checkpoint folders
* generated Parquet output
* generated CSV output
* large sample data
* local IDE settings unless the team intentionally shares them

Recommended `.gitignore` entries:

```gitignore
.env
.venv/
__pycache__/
*.pyc
logs/
data/raw/
data/curated/
data/rejects/
data/checkpoints/
airflow/logs/
spark-warehouse/
.idea/
.vscode/
```

If the project needs sample data, keep it tiny and place it in a clearly named folder such as `tests/fixtures/`.

## Definition Of Ready For Merge

A PR is ready to merge when:

* The branch is up to date with `main`.
* The PR has one clear purpose.
* The project still starts or the changed component can be tested independently.
* Generated data, logs, checkpoints, and secrets are not included.
* The README or runbook is updated if commands changed.
* At least one teammate reviewed it.

## Phase 1 Team Checklist

Before the final demo, confirm that `main` can do these from a fresh clone:

* Start the local services with Docker Compose.
* Create or use the configured Kafka-compatible topic.
* Run the event producer.
* Run Spark Structured Streaming ingest.
* Confirm raw Parquet output is written.
* Confirm the checkpoint path is used.
* Run the Spark summary job.
* Trigger or demonstrate the Airflow DAG.
* Explain how to clean generated local output and rerun.

If `main` cannot do these, stop adding features and fix integration first.
