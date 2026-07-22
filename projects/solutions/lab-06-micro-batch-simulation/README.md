# Lab 6 Solution - Micro-Batch Simulation

Original lab: [`../../labs/lab-06-micro-batch-simulation.md`](../../labs/lab-06-micro-batch-simulation.md)

## Files

- `jobs/process_next_batch.py` selects and processes one unprocessed batch.
- `incoming/batch_001` through `batch_003` are the initial event batches.
- `examples/batch_004` demonstrates a batch arriving later.
- `tests/test_process_next_batch.py` checks state, ordering, idle behavior, and
  Spark aggregation.
- `requirements.txt` pins the Java 11-compatible PySpark runtime.

The state file is updated only after output succeeds. A failed batch therefore
remains eligible for retry, while Spark's overwrite mode safely replaces output
left by a run that failed just before recording its state.

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

Then install and verify the runtime:

```bash
python -m pip install --upgrade pip
python -m pip install --no-cache-dir -r requirements.txt
python -c "import pyspark; print(f'PySpark {pyspark.__version__}')"
java -version
spark-submit --version
```

This solution uses PySpark 3.5.9 with Java 11. On a Homebrew Mac where OpenJDK
11 is keg-only, configure only the current Bash session:

```bash
export JAVA_HOME="$(brew --prefix openjdk@11)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

## 2. Process the Initial Batches

Run the same command four times:

```bash
spark-submit jobs/process_next_batch.py \
  --incoming-dir incoming \
  --output-dir output \
  --state-dir _state
```

Expected progression:

1. The first run processes only `batch_001`.
2. The second run processes only `batch_002`.
3. The third run processes only `batch_003`.
4. The fourth run prints `No new batches to process.` and exits before creating
   a Spark session.

The three summaries contain these rows:

```text
batch_001: page_view/web=1, video_play/mobile=1
batch_002: page_view/web=1, purchase/web=1
batch_003: add_to_cart/mobile=1, purchase/mobile=1
```

Inspect the generated datasets and state:

```bash
find output -maxdepth 2 -type f -print
cat output/batch_001/part-*.csv
cat output/batch_002/part-*.csv
cat output/batch_003/part-*.csv
cat _state/processed_batches.txt
```

Expected state:

```text
batch_001
batch_002
batch_003
```

## 3. Add a Later Batch

Copy the supplied example as if another system had just delivered it:

```bash
cp -R examples/batch_004 incoming/
```

Run the same `spark-submit` command again. It should process only `batch_004`
and produce the summary row `video_pause,mobile,1`. The state file should then
contain all four batch names. A subsequent run should again report no new
batches.

## 4. Run the Automated Tests

```bash
python -m unittest discover -s tests -v
```

Expected result:

```text
Ran 5 tests

OK
```

## State and Delivery Semantics

Sorting batch names makes processing order deterministic. Rewriting the whole
small state file through an atomic replacement prevents partial state lines and
keeps repeated updates idempotent. This single-process simulation does not
implement a distributed lock, so two copies of the job should not run at once.

The design provides at-least-once processing around a crash: output may be
rewritten if a process stops after the Spark write but before the state update.
Overwrite mode makes that retry safe for this lab. Production stream processors
usually use transactional sinks, checkpoints, and coordinated concurrency.

### Native Windows note

Spark 3.5's Hadoop CSV writer normally requires `HADOOP_HOME` and
`winutils.exe` on native Windows. When `HADOOP_HOME` is unset, this solution
uses Spark for input and aggregation, then writes only the tiny summary with
Python's CSV library. macOS, Linux, WSL, and configured Windows systems use
Spark's CSV writer.

## Deliverable Explanation

This resembles stream processing because data arrives as a sequence of small
batches and the job retains progress between runs. Each invocation handles only
new data, creates an independently inspectable result, and leaves earlier
outputs unchanged. Unlike continuous Structured Streaming, the simulation must
be invoked repeatedly and its text state file is suitable only for one process.

## Runtime Evidence

Run the commands above and save real terminal output or screenshots here if a
submission requires them. Runtime evidence is intentionally not prefilled.

## Cleanup

Remove only the generated output, state, and copied example batch:

```bash
rm -r output _state incoming/batch_004
deactivate
```
