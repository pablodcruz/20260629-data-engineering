# StreamFlow Lab Solutions

This directory contains reference implementations for the labs in
[`../labs`](../labs/README.md). Each solution lives in its own folder and
includes the files needed to run the lab, verification commands, expected
results, and written responses to the lab questions.

## How to Use the Solutions

1. Attempt the corresponding lab before reading its solution.
2. Open the solution folder and read its `README.md`.
3. Run the listed verification commands from that solution folder.
4. Compare behavior and design choices, rather than copying files blindly.
5. Add real terminal output or screenshots only after running the solution.

Generated output, screenshots, credentials, and machine-specific files should
not be committed unless a lab explicitly requires them.

## Progress

| Lab | Topic | Status |
| --- | --- | --- |
| 01 | [Docker Compose Warmup](lab-01-docker-compose-warmup/README.md) | Complete; runtime verification pending |
| 02 | [Kafka Producer and Consumer](lab-02-kafka-producer-consumer/README.md) | Complete; broker verified, Python test pending |
| 03 | [Event Schema Design](lab-03-event-schema-design/README.md) | Complete and verified |
| 04 | [PySpark DataFrames](lab-04-pyspark-dataframes/README.md) | Complete; Windows output fallback verified |
| 04B | [Spark SQL](lab-04b-spark-sql/README.md) | Complete; Spark runtime verification pending |
| 05 | [Spark Batch Job](lab-05-spark-batch-job/README.md) | Complete and verified on macOS |
| 06 | [Micro-Batch Simulation](lab-06-micro-batch-simulation/README.md) | Complete and verified on macOS |
| 07 | [Airflow DAG Basics](lab-07-airflow-dag-basics/README.md) | Complete and verified on macOS |
| 08 | Airflow + Spark Submit | Planned |
| 09 | Data Quality Checks | Planned |
| 10 | End-to-End Mini Pipeline | Planned |
| 11 | Debugging Distributed Systems | Planned |
| 12 | Team Git Workflow | Planned |
| 13 | Kafka Partitions, Keys, and Consumer Groups | Planned |
| 14 | Airflow Parameters, Dynamic Tasks, Connections, and Hooks | Planned |

## Solution Standards

Every solution should include:

- A link to the original lab handout.
- All authored source and configuration files.
- Teaching-focused comments for unfamiliar configuration and important design
  choices, without commenting obvious syntax.
- Bash-first run and verification commands, using Git Bash paths when an
  absolute Windows workspace path is needed.
- Exact commands for running and verifying the work.
- Expected results without fabricated runtime evidence.
- Answers to explanations and reflection questions.
- Cleanup instructions where the lab starts services or creates generated data.
