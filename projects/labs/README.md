# StreamFlow Phase 1 Labs

These labs prepare associates for **StreamFlow Phase 1 - Containerized Stream Processing Platform**.
The sequence moves from individual tools to an end-to-end mini pipeline.

## How to Use These Labs

Each lab is written as a standalone handout.
Associates should be able to complete the work using the commands, code snippets, and checkpoints in the lab file.

Recommended workflow:

* Read the objective and scenario first.
* Create the suggested working folder.
* Copy the provided starter files exactly before making changes.
* Run each command in order.
* Save screenshots, terminal output, or short notes for the deliverables.
* Keep a short debugging log when something fails.

## Common Assumptions

These labs assume associates have access to:

* Docker Desktop or Docker Engine with Docker Compose.
* Python 3.10 or later.
* Git.
* Git Bash or another Bash-compatible terminal.
* Java 11 or later for local Spark work, unless Spark is run inside a container.

Commands are shown in a terminal-friendly style.
Folder and file setup commands assume Git Bash.

## Suggested Workspace

Create one workspace folder for lab artifacts so generated files do not clutter the notes folder:

```bash
mkdir -p streamflow-lab-work
cd streamflow-lab-work
```

Inside that folder, create one subfolder per lab, such as `lab-01-docker`, `lab-02-kafka`, and so on.

## Lab Sequence

| Lab | Topic | Main Skill |
| --- | ----- | ---------- |
| 1 | [Docker Compose Warmup](lab-01-docker-compose-warmup.md) | Running and inspecting multi-container services |
| 2 | [Kafka Producer and Consumer](lab-02-kafka-producer-consumer.md) | Sending and reading event messages |
| 3 | [Event Schema Design](lab-03-event-schema-design.md) | Defining clean event payloads |
| 4 | [PySpark DataFrames](lab-04-pyspark-dataframes.md) | Transforming structured event data |
| 5 | [Spark Batch Job](lab-05-spark-batch-job.md) | Running Spark code with `spark-submit` |
| 6 | [Micro-Batch Simulation](lab-06-micro-batch-simulation.md) | Processing data in small batches |
| 7 | [Airflow DAG Basics](lab-07-airflow-dag-basics.md) | Creating scheduled workflows |
| 8 | [Airflow + Spark Submit](lab-08-airflow-spark-submit.md) | Orchestrating Spark jobs |
| 9 | [Data Quality Checks](lab-09-data-quality-checks.md) | Validating pipeline outputs |
| 10 | [End-to-End Mini Pipeline](lab-10-end-to-end-mini-pipeline.md) | Connecting the core platform pieces |
| 11 | [Debugging Distributed Systems](lab-11-debugging-distributed-systems.md) | Reading logs and fixing common issues |
| 12 | [Team Git Workflow](lab-12-team-git-workflow.md) | Collaborating safely on shared code |

## Suggested Pacing

| Week | Labs |
| ---- | ---- |
| Week 1 | Labs 1-4 |
| Week 2 | Labs 5-8 |
| Week 3 | Labs 9-12 |

## Deliverable Standard (Optional, but recommended for your portfolio)

Associates should create a dedicated **Labs** repository on GitHub and submit their work through that repo.

Each lab should be organized in its own folder and include a `README.md` file.

For each lab, the `README.md` should include:

- The files they created or modified
- The command used to run or test the lab
- A short note explaining what worked successfully
- A short note describing one error, question, or debugging step they encountered

## Instructor Notes

* Keep each lab small enough to finish in one class block or one homework session.
* Use the same event theme across labs so the work compounds.
* Encourage teams to document commands and errors as they go.
* Treat failed runs as useful debugging material, not wasted time.
* Before the final project build, have each team identify which lab artifacts they can reuse.
