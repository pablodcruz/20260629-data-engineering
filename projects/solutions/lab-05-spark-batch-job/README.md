# Lab 5 Solution - Spark Batch Job

Original lab: [`../../labs/lab-05-spark-batch-job.md`](../../labs/lab-05-spark-batch-job.md)

## Files

- `jobs/process_events.py` validates, summarizes, and writes event datasets.
- `data/events.jsonl` contains four accepted and two rejected events.
- `tests/test_process_events.py` checks classification, reasons, and aggregation.
- `requirements.txt` pins the same Java 11-compatible PySpark release as Lab 4.

Input and output paths are command-line arguments so the same job can process
different locations without a source-code change.

## 1. Create the Environment

From this solution directory, run:

```bash
python3 -m venv .venv
```

Activate it on macOS or Linux:

```bash
source .venv/bin/activate
```

In Git Bash on Windows, use:

```bash
source .venv/Scripts/activate
```

Then install and inspect the pinned runtime:

```bash
python -m pip install --upgrade pip
python -m pip install --no-cache-dir -r requirements.txt
python -c "import pyspark; print(f'PySpark {pyspark.__version__}')"
java -version
spark-submit --version
```

This solution uses PySpark 3.5.9 with Java 11. On a Homebrew-based Mac where
OpenJDK 11 is keg-only, select it for the current shell without changing the
system Java configuration:

```bash
export JAVA_HOME="$(brew --prefix openjdk@11)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

## 2. Run with Spark Submit

```bash
spark-submit jobs/process_events.py \
  --input data/events.jsonl \
  --summary-output output/event_summary \
  --bad-output output/bad_events
```

Expected terminal results:

- The summary table contains four groups, each with `event_count=1` and
  `unique_users=1`.
- The rejected table contains `evt_bad_001` with `missing user_id` and
  `evt_bad_002` with `invalid event_ts`.
- The final lines name the writer used for both output datasets.

## 3. Inspect the Output Datasets

Spark writes each dataset as a directory containing a generated `part-*` file
and metadata. Inspect the files with Bash:

```bash
find output/event_summary output/bad_events -maxdepth 1 -type f -print
cat output/event_summary/part-*.csv
cat output/bad_events/part-*.csv
```

Expected summary rows, ignoring the generated part filename:

```text
event_type,source,event_count,unique_users
page_view,web,1,1
purchase,mobile,1,1
purchase,web,1,1
video_play,mobile,1,1
```

Expected rejected rows:

```text
event_id,event_type,user_id,event_ts,source,reject_reason
evt_bad_001,purchase,"",2026-06-30T14:05:00Z,web,missing user_id
evt_bad_002,page_view,user_105,not-a-timestamp,web,invalid event_ts
```

The portable Windows writer may represent the same empty `user_id` as two
adjacent commas instead of a quoted empty string. Both are valid CSV.

### Native Windows note

Spark 3.5's Hadoop CSV writer normally requires `HADOOP_HOME` and
`winutils.exe` on native Windows. When `HADOOP_HOME` is unset, this solution
uses Spark for reading and transformations, then safely writes only these tiny
result sets with Python's CSV library. macOS, Linux, WSL, and configured Windows
systems use Spark's CSV writer.

## 4. Run the Automated Tests

```bash
python -m unittest discover -s tests -v
```

Expected result:

```text
Ran 3 tests

OK
```

## Reflection Answers

### Why pass paths as arguments instead of hardcoding them?

Arguments let one tested job run in development, test, and production with
different storage locations. Schedulers can supply paths for each run, and the
code remains reusable instead of being edited and redeployed for every dataset.

### What changes when the job runs on a cluster?

The submit command selects a cluster manager and deploy mode, and input/output
paths must refer to storage reachable by every executor, such as object storage
or HDFS rather than a laptop-only path. Resource settings, dependency shipping,
permissions, logging, and monitoring also become deployment concerns. The
DataFrame transformations can remain the same.

## Runtime Evidence

Run the commands above and save real terminal output or a screenshot here if a
submission requires it. Runtime evidence is intentionally not prefilled.

## Cleanup

The job uses overwrite mode, so rerunning it safely replaces its own datasets.
To remove generated output when it is no longer needed:

```bash
rm -r output/event_summary output/bad_events
deactivate
```
