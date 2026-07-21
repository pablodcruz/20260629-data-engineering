# Lab 4B Solution - Spark SQL

Original lab: [`../../labs/lab-04b-spark-sql.md`](../../labs/lab-04b-spark-sql.md)

## Files

- `data/events.jsonl` contains ten application events across two dates.
- `data/users.jsonl` contains six user lookup records.
- `spark_sql_analysis.py` registers views and runs the SQL analysis.
- `tests/test_spark_sql_analysis.py` verifies the queries and partition layout.
- `requirements.txt` pins the Java 11-compatible PySpark release.

## 1. Open the Solution Folder

```bash
cd "/c/Users/Pablo/Documents/20260629-data-engineering-master/20260629-data-engineering-master/projects/solutions/lab-04b-spark-sql"
```

## 2. Create the Environment

```bash
python -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install --no-cache-dir -r requirements.txt
```

PySpark's package is large. If Lab 4's environment is still active and already
contains PySpark 3.5.9, you can reuse it instead of downloading PySpark again.

Confirm the runtimes:

```bash
python -c "import pyspark; print(f'PySpark {pyspark.__version__}')"
java -version
```

## 3. Run the Analysis

```bash
python spark_sql_analysis.py
```

Important expected results:

- The web filter returns five events.
- The page-view/web group contains three events from two unique users.
- Purchase revenue contains an `unknown/unknown` row worth `49.99`.
- The anti join returns `evt_010` for `user_999`.
- The set operation returns `user_104` and `user_999`.
- The enriched view contains ten rows.

## 4. Inspect the Partitioned Output

```bash
find output/enriched_events_by_date -maxdepth 2 -type f -print
```

The output contains two partition directories:

```text
event_date=2026-07-06/
event_date=2026-07-07/
```

On Linux, macOS, WSL, or Windows with Hadoop helpers configured, the directory
contains real Parquet part files written by Spark. On native Windows without
`HADOOP_HOME`, the safe teaching fallback writes JSON Lines files instead:

```bash
cat output/enriched_events_by_date/event_date=*/part-*-local.jsonl
```

The fallback preserves the partition layout but is not Parquet. Use WSL, a
container, or a trusted Hadoop installation when the exact Parquet deliverable
is required. The solution intentionally does not download an unofficial
`winutils.exe` executable.

## 5. Run the Automated Tests

```bash
python -m unittest discover -s tests -v
```

Expected result:

```text
Ran 6 tests

OK
```

## Practice Answers

### 1. Why create temporary views?

`spark.sql()` resolves table names through Spark's catalog. Registering the
DataFrames as `events` and `users` gives SQL statements stable names to query.
The views last only for the current Spark session and do not copy the data.

### 2. DataFrame transformation versus SQL query

They are two interfaces to the same Spark SQL engine. Both produce logical
plans that Spark analyzes and optimizes before execution. DataFrames compose
through Python methods; SQL expresses the plan as a query string.

### 3. Why use a left join for revenue?

A left join retains every purchase even when its user lookup is missing. The
query can then label the missing plan and region as `unknown`. An inner join
would silently discard the `user_999` purchase and understate revenue by 49.99.

### 4. What does the left anti join return?

It returns rows from the left side that have no matching join key on the right.
Here it identifies events whose users are absent from the lookup dataset.

### 5. Why partition by event date?

Date-filtered queries can skip unrelated directories instead of scanning every
record. This partition pruning reduces input work when the dataset is large and
queries commonly constrain dates.

### 6. When is caching useful?

Caching helps when an expensive DataFrame will be reused by multiple actions.
It consumes executor memory and may cost more than recomputation, so caching
every intermediate DataFrame can evict useful data or create memory pressure.
Always unpersist cached data when it is no longer needed.

## Cleanup

```bash
deactivate
```

Generated `output/` and `.venv/` directories are ignored by Git.
