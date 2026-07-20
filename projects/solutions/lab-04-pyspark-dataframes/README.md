# Lab 4 Solution - PySpark DataFrames

Original lab: [`../../labs/lab-04-pyspark-dataframes.md`](../../labs/lab-04-pyspark-dataframes.md)

## Files

- `data/events.jsonl` contains five valid and two invalid events.
- `transform_events.py` reads, validates, summarizes, and writes the data.
- `tests/test_transform_events.py` checks the split, errors, and aggregation.
- `requirements.txt` pins the Java 11-compatible PySpark release.

## 1. Open the Solution Folder

```bash
cd "/c/Users/Pablo/Documents/20260629-data-engineering-master/20260629-data-engineering-master/projects/solutions/lab-04-pyspark-dataframes"
```

## 2. Create the Environment

```bash
python -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install --no-cache-dir -r requirements.txt
```

PySpark's download is large. `--no-cache-dir` avoids retaining a second cached
copy of the package after installation.

Confirm both required runtimes:

```bash
python -c "import pyspark; print(f'PySpark {pyspark.__version__}')"
java -version
```

This solution uses PySpark 3.5.9 because it supports Java 11. Current Spark 4.x
releases require Java 17 or newer.

## 3. Run the Transformation

```bash
python transform_events.py data/events.jsonl output/event_summary
```

The important expected results are:

- Five rows appear under `Valid records`.
- Two rows appear under `Invalid records and reasons`.
- The page-view/web summary has `event_count=2` and `unique_users=1`.
- The script reports that it wrote the dataset using either Spark's CSV writer
  or the portable Windows fallback.

The other summary groups are add-to-cart/mobile, purchase/web, and
video-play/mobile, each with one event and one unique user.

## 4. Inspect the CSV Dataset

Spark writes a directory containing part files and metadata rather than one
file. List the generated files and display the CSV part file:

```bash
find output/event_summary -maxdepth 1 -type f -print
cat output/event_summary/part-*.csv
```

Expected CSV rows, ignoring the generated part filename:

```text
event_type,source,event_count,unique_users,first_event_time,last_event_time
add_to_cart,mobile,1,1,2026-06-30T14:06:00.000Z,2026-06-30T14:06:00.000Z
page_view,web,2,1,2026-06-30T14:00:00.000Z,2026-06-30T14:05:00.000Z
purchase,web,1,1,2026-06-30T14:03:00.000Z,2026-06-30T14:03:00.000Z
video_play,mobile,1,1,2026-06-30T14:01:00.000Z,2026-06-30T14:01:00.000Z
```

### Native Windows note

Spark 3.5's Hadoop CSV writer normally expects `HADOOP_HOME` and `winutils.exe`
on native Windows. If `HADOOP_HOME` is unset, this solution safely collects only
the four-row aggregate and writes `part-00000-local.csv` plus `_SUCCESS` with
Python's standard library. All reading, validation, filtering, and aggregation
still happen in Spark. On Linux, macOS, WSL, or a configured Hadoop environment,
Spark's distributed CSV writer is used normally.

## 5. Run the Automated Tests

```bash
python -m unittest discover -s tests -v
```

Expected result:

```text
Ran 3 tests

OK
```

## Why Spark Writes a Folder

A Spark DataFrame is normally distributed across multiple partitions. Each
partition can be processed and written by a different executor, so Spark treats
an output path as a dataset directory containing one or more `part-*` files plus
metadata such as `_SUCCESS`. This solution uses `coalesce(1)` only to make the
tiny lab output easy to inspect. For large data, forcing one partition would
discard parallelism and create a bottleneck.

## Cleanup

Stop using the virtual environment when finished:

```bash
deactivate
```

The transformation uses overwrite mode, so it is safe to rerun without deleting
the previous output first.
