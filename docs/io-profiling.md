# Spark I/O overhead experiments (native Spark)

This note focuses on **measuring where time/bytes go** in Spark runs, before attributing any
speedup/slowdown to SCache.

## 0) Use Spark event logs as the ground truth

Spark already records per-task metrics (shuffle read/write, spill, GC, etc.) into the event log.
This repo provides a small parser:

```bash
cd $HOME/SCache
python3 scripts/spark_eventlog_io_breakdown.py /home/yxz/spark-apps/logs/spark-events/app-*
```

Tip: pick the latest log:

```bash
latest=$(ls -t /home/yxz/spark-apps/logs/spark-events/app-* | head -n1)
python3 scripts/spark_eventlog_io_breakdown.py "$latest"
```

What you get (per stage):

- `shuffleWrite`: bytes + `Shuffle Write Time` (Spark counter, nanoseconds -> ms)
- `shuffleRead`: local/remote bytes + `Fetch Wait Time`
- `spill`: memory/disk bytes spilled
- `timeShare(execRun)`: rough percentages relative to summed `Executor Run Time`

Notes:

- `Executor Run Time` is **summed over tasks**, so it is not the same as wall-clock time (parallelism).
- Spark does not expose a dedicated “shuffle commit” timer; `Shuffle Write Time` is the closest
  built-in counter for “time spent writing shuffle output”.

## 1) Single-node: estimate “shuffle write / local disk” cost

Goal: quantify the cost of writing shuffle data (and spilling) on a single host.

Key idea: vary only the **local disk backend** while keeping compute/shuffle shape fixed.

Suggested workload:

- `org.apache.spark.examples.GroupByTest` (already used in this workspace)

Suggested knobs:

- Put Spark local dirs on a disk path vs tmpfs:
  - disk: `SPARK_LOCAL_DIRS=/home/yxz/spark-apps/tmp/spark-local`
  - tmpfs: `SPARK_LOCAL_DIRS=/dev/shm/spark-local` (ensure enough tmpfs space)
- Increase data / partitions until you see `diskBytesSpilled` and large `shuffleWriteBytes`.

Run and compare:

- Stage with `Task Type=ShuffleMapTask` should show non-zero `shuffleWrite`.
- Look at `shuffleWrite timeShare(execRun)` and `spill` bytes.

Optional system counters (single box):

```bash
iostat -dx 1
sar -n DEV 1
pidstat -dru 1
```

## 2) Multi-node: isolate Netty shuffle network overhead

Goal: quantify network + remote fetch wait in shuffle reads.

The most direct Spark-side counter is:

- `Shuffle Read Metrics.Fetch Wait Time` (per task, summed by stage)
- `Remote Bytes Read` / `Remote Blocks Fetched`

Make sure the job actually has **remote blocks** (executors spread across hosts).

Also note a common “gotcha” for single-host multi-executor runs:

- Spark 3.x defaults to `spark.shuffle.readHostLocalDisk=true`, which makes same-host blocks
  be read directly from disk (not via Netty), so `remoteBlocksFetched` can stay at 0.

## 3) Multi-node: estimate HDFS I/O overhead

Spark’s task metrics record:

- `Input Metrics.Bytes Read` (bytes, but not “time spent in HDFS”)

So for HDFS you typically combine:

- Spark stage wall time + task `Executor Run Time`
- input bytes (`Input Metrics`)
- OS-level network/disk counters on the relevant nodes (executors + datanodes):
  - `sar -n DEV 1`
  - `iostat -dx 1`

If you want HDFS time breakdown, you’ll generally need either:

- Hadoop-level FS statistics / tracing, or
- eBPF/perf-based syscall/block I/O profiling

## Baseline vs SCache

For baseline (native Spark), ensure:

- `spark.scache.enable=false`

Then repeat the same workload/config and compare the eventlog summaries.

