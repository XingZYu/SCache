#!/usr/bin/env python3

import argparse
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _human_bytes(num: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    value = float(num)
    for u in units:
        if abs(value) < 1024.0 or u == units[-1]:
            if u == "B":
                return f"{int(value)} {u}"
            return f"{value:.2f} {u}"
        value /= 1024.0
    return f"{value:.2f} PiB"


def _human_ms(ms: float) -> str:
    if ms < 1000:
        return f"{ms:.0f} ms"
    s = ms / 1000.0
    if s < 60:
        return f"{s:.2f} s"
    m = s / 60.0
    if m < 60:
        return f"{m:.2f} min"
    h = m / 60.0
    return f"{h:.2f} h"


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


@dataclass
class StageMeta:
    stage_id: int
    attempt_id: int
    name: str = ""
    submission_time_ms: Optional[int] = None
    completion_time_ms: Optional[int] = None
    num_tasks: Optional[int] = None

    def wall_ms(self) -> Optional[int]:
        if self.submission_time_ms is None or self.completion_time_ms is None:
            return None
        return max(0, self.completion_time_ms - self.submission_time_ms)


@dataclass
class StageAgg:
    # Task time components (milliseconds unless noted).
    tasks: int = 0
    executor_run_ms: int = 0
    executor_deserialize_ms: int = 0
    gc_ms: int = 0
    result_ser_ms: int = 0

    # Shuffle read.
    shuffle_read_fetch_wait_ms: int = 0
    shuffle_read_remote_req_ms: int = 0
    shuffle_read_remote_blocks: int = 0
    shuffle_read_local_blocks: int = 0
    shuffle_read_remote_bytes: int = 0
    shuffle_read_remote_bytes_to_disk: int = 0
    shuffle_read_local_bytes: int = 0
    shuffle_read_records: int = 0
    scache_fetch_count: int = 0
    scache_fetch_time_ns: int = 0
    scache_fetch_miss_count: int = 0
    scache_fetch_retry_time_ns: int = 0
    scache_batch_lookup_count: int = 0
    scache_cxl_reservation_count: int = 0
    scache_cxl_reservation_wait_ns: int = 0
    scache_ipc_file_read_bytes: int = 0
    scache_ipc_pool_read_bytes: int = 0
    scache_direct_cxl_read_bytes: int = 0
    scache_byte_array_read_bytes: int = 0

    # Shuffle write.
    shuffle_write_bytes: int = 0
    shuffle_write_records: int = 0
    shuffle_write_time_ns: int = 0
    scache_pool_alloc_count: int = 0
    scache_pool_alloc_time_ns: int = 0
    scache_pool_commit_count: int = 0
    scache_pool_commit_time_ns: int = 0

    # Spill.
    memory_spilled_bytes: int = 0
    disk_spilled_bytes: int = 0

    # Input / output.
    input_bytes: int = 0
    input_records: int = 0
    output_bytes: int = 0
    output_records: int = 0

    def shuffle_write_ms(self) -> float:
        # Spark records shuffle write time in nanoseconds.
        return float(self.shuffle_write_time_ns) / 1_000_000.0

    def est_other_ms(self) -> int:
        # executorRunTime is "total task time". The other buckets are subsets; this is a rough
        # residual to show what is not captured by the explicit counters above.
        other = (
            self.executor_run_ms
            - self.executor_deserialize_ms
            - self.gc_ms
            - self.result_ser_ms
            - self.shuffle_read_fetch_wait_ms
            - int(round(self.shuffle_write_ms()))
        )
        return max(0, other)


def _iter_eventlog_paths(paths: List[str]) -> Iterable[str]:
    for p in paths:
        if os.path.isdir(p):
            for name in sorted(os.listdir(p)):
                full = os.path.join(p, name)
                if os.path.isfile(full):
                    yield full
        else:
            yield p


def _parse_eventlog(path: str) -> Tuple[Dict[str, str], Optional[int], Optional[int], Dict[int, StageMeta], Dict[int, StageAgg]]:
    spark_props: Dict[str, str] = {}
    app_start_ms: Optional[int] = None
    app_end_ms: Optional[int] = None
    stage_meta: Dict[int, StageMeta] = {}
    stage_agg: Dict[int, StageAgg] = defaultdict(StageAgg)

    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except Exception as e:
                raise RuntimeError(f"Failed to parse JSON at {path}:{line_no}: {e}") from e

            et = ev.get("Event", "")
            if et == "SparkListenerEnvironmentUpdate":
                props = ev.get("Spark Properties") or {}
                if isinstance(props, dict):
                    for k, v in props.items():
                        spark_props[str(k)] = str(v)

            elif et == "SparkListenerApplicationStart":
                app_start_ms = _safe_int(ev.get("Timestamp"), app_start_ms or 0)

            elif et == "SparkListenerApplicationEnd":
                app_end_ms = _safe_int(ev.get("Timestamp"), app_end_ms or 0)

            elif et == "SparkListenerStageCompleted":
                info = ev.get("Stage Info") or {}
                sid = _safe_int(info.get("Stage ID"), -1)
                if sid >= 0:
                    meta = StageMeta(
                        stage_id=sid,
                        attempt_id=_safe_int(info.get("Stage Attempt ID"), 0),
                        name=str(info.get("Stage Name") or ""),
                        submission_time_ms=_safe_int(info.get("Submission Time"), None) if info.get("Submission Time") is not None else None,
                        completion_time_ms=_safe_int(info.get("Completion Time"), None) if info.get("Completion Time") is not None else None,
                        num_tasks=_safe_int(info.get("Number of Tasks"), None) if info.get("Number of Tasks") is not None else None,
                    )
                    stage_meta[sid] = meta

            elif et == "SparkListenerTaskEnd":
                sid = _safe_int(ev.get("Stage ID"), -1)
                if sid < 0:
                    continue
                agg = stage_agg[sid]
                agg.tasks += 1
                tm = ev.get("Task Metrics") or {}
                if not isinstance(tm, dict):
                    continue

                agg.executor_deserialize_ms += _safe_int(tm.get("Executor Deserialize Time"))
                agg.executor_run_ms += _safe_int(tm.get("Executor Run Time"))
                agg.gc_ms += _safe_int(tm.get("JVM GC Time"))
                agg.result_ser_ms += _safe_int(tm.get("Result Serialization Time"))
                agg.memory_spilled_bytes += _safe_int(tm.get("Memory Bytes Spilled"))
                agg.disk_spilled_bytes += _safe_int(tm.get("Disk Bytes Spilled"))

                sr = tm.get("Shuffle Read Metrics") or {}
                if isinstance(sr, dict):
                    agg.shuffle_read_remote_blocks += _safe_int(sr.get("Remote Blocks Fetched"))
                    agg.shuffle_read_local_blocks += _safe_int(sr.get("Local Blocks Fetched"))
                    agg.shuffle_read_fetch_wait_ms += _safe_int(sr.get("Fetch Wait Time"))
                    agg.shuffle_read_remote_bytes += _safe_int(sr.get("Remote Bytes Read"))
                    agg.shuffle_read_remote_bytes_to_disk += _safe_int(sr.get("Remote Bytes Read To Disk"))
                    agg.shuffle_read_local_bytes += _safe_int(sr.get("Local Bytes Read"))
                    agg.shuffle_read_records += _safe_int(sr.get("Total Records Read"))
                    agg.shuffle_read_remote_req_ms += _safe_int(sr.get("Remote Requests Duration"))
                    agg.scache_fetch_count += _safe_int(sr.get("SCache Fetch Count"))
                    agg.scache_fetch_time_ns += _safe_int(sr.get("SCache Fetch Time Ns"))
                    agg.scache_fetch_miss_count += _safe_int(sr.get("SCache Fetch Miss Count"))
                    agg.scache_fetch_retry_time_ns += _safe_int(
                        sr.get("SCache Fetch Retry Time Ns"))
                    agg.scache_batch_lookup_count += _safe_int(
                        sr.get("SCache Batch Lookup Count"))
                    agg.scache_cxl_reservation_count += _safe_int(
                        sr.get("SCache CXL Reservation Count"))
                    agg.scache_cxl_reservation_wait_ns += _safe_int(
                        sr.get("SCache CXL Reservation Wait Ns"))
                    agg.scache_ipc_file_read_bytes += _safe_int(
                        sr.get("SCache IPC File Read Bytes"))
                    agg.scache_ipc_pool_read_bytes += _safe_int(
                        sr.get("SCache IPC Pool Read Bytes"))
                    agg.scache_direct_cxl_read_bytes += _safe_int(
                        sr.get("SCache Direct CXL Read Bytes"))
                    agg.scache_byte_array_read_bytes += _safe_int(
                        sr.get("SCache Byte Array Read Bytes"))

                sw = tm.get("Shuffle Write Metrics") or {}
                if isinstance(sw, dict):
                    agg.shuffle_write_bytes += _safe_int(sw.get("Shuffle Bytes Written"))
                    agg.shuffle_write_records += _safe_int(sw.get("Shuffle Records Written"))
                    agg.shuffle_write_time_ns += _safe_int(sw.get("Shuffle Write Time"))
                    agg.scache_pool_alloc_count += _safe_int(
                        sw.get("SCache Pool Alloc Count"))
                    agg.scache_pool_alloc_time_ns += _safe_int(
                        sw.get("SCache Pool Alloc Time Ns"))
                    agg.scache_pool_commit_count += _safe_int(
                        sw.get("SCache Pool Commit Count"))
                    agg.scache_pool_commit_time_ns += _safe_int(
                        sw.get("SCache Pool Commit Time Ns"))

                inp = tm.get("Input Metrics") or {}
                if isinstance(inp, dict):
                    agg.input_bytes += _safe_int(inp.get("Bytes Read"))
                    agg.input_records += _safe_int(inp.get("Records Read"))

                outp = tm.get("Output Metrics") or {}
                if isinstance(outp, dict):
                    agg.output_bytes += _safe_int(outp.get("Bytes Written"))
                    agg.output_records += _safe_int(outp.get("Records Written"))

    return spark_props, app_start_ms, app_end_ms, stage_meta, stage_agg


def _format_stage_line(meta: StageMeta, agg: StageAgg) -> str:
    wall_ms = meta.wall_ms()
    wall_part = _human_ms(wall_ms) if wall_ms is not None else "?"
    exec_part = _human_ms(agg.executor_run_ms) if agg.executor_run_ms else "0 ms"

    parts = [
        f"stage {meta.stage_id} ({agg.tasks} tasks, wall={wall_part}, sumExec={exec_part}) {meta.name}".rstrip()
    ]

    def add_kv(label: str, value: str) -> None:
        parts.append(f"  - {label}: {value}")

    if agg.shuffle_write_bytes or agg.shuffle_write_time_ns:
        add_kv("shuffleWrite", f"{_human_bytes(agg.shuffle_write_bytes)}, time={_human_ms(agg.shuffle_write_ms())}")

    if (agg.scache_pool_alloc_count or agg.scache_pool_commit_count or
            agg.scache_pool_alloc_time_ns or agg.scache_pool_commit_time_ns):
        add_kv(
            "scacheMapMetadata",
            f"allocRPCs={agg.scache_pool_alloc_count}, "
            f"allocTime={_human_ms(agg.scache_pool_alloc_time_ns / 1_000_000.0)}, "
            f"commitRPCs={agg.scache_pool_commit_count}, "
            f"commitTime={_human_ms(agg.scache_pool_commit_time_ns / 1_000_000.0)}",
        )

    if (agg.shuffle_read_local_bytes or agg.shuffle_read_remote_bytes or agg.shuffle_read_fetch_wait_ms or
            agg.shuffle_read_local_blocks or agg.shuffle_read_remote_blocks):
        add_kv(
            "shuffleRead",
            f"local={_human_bytes(agg.shuffle_read_local_bytes)} ({agg.shuffle_read_local_blocks} blocks), "
            f"remote={_human_bytes(agg.shuffle_read_remote_bytes)} ({agg.shuffle_read_remote_blocks} blocks), "
            f"fetchWait={_human_ms(agg.shuffle_read_fetch_wait_ms)}",
        )

    if (agg.scache_fetch_count or agg.scache_ipc_file_read_bytes or
            agg.scache_ipc_pool_read_bytes or agg.scache_direct_cxl_read_bytes or
            agg.scache_byte_array_read_bytes):
        add_kv(
            "scacheReadPaths",
            f"file={_human_bytes(agg.scache_ipc_file_read_bytes)}, "
            f"pool={_human_bytes(agg.scache_ipc_pool_read_bytes)}, "
            f"cxl={_human_bytes(agg.scache_direct_cxl_read_bytes)}, "
            f"byteArray={_human_bytes(agg.scache_byte_array_read_bytes)}, "
            f"fetches={agg.scache_fetch_count}, misses={agg.scache_fetch_miss_count}, "
            f"batchLookups={agg.scache_batch_lookup_count}, "
            f"cxlReservations={agg.scache_cxl_reservation_count}, "
            f"cxlWait={_human_ms(agg.scache_cxl_reservation_wait_ns / 1_000_000.0)}, "
            f"fetchTime={_human_ms(agg.scache_fetch_time_ns / 1_000_000.0)}, "
            f"retryTime={_human_ms(agg.scache_fetch_retry_time_ns / 1_000_000.0)}",
        )

    if agg.disk_spilled_bytes or agg.memory_spilled_bytes:
        add_kv("spill", f"mem={_human_bytes(agg.memory_spilled_bytes)}, disk={_human_bytes(agg.disk_spilled_bytes)}")

    if agg.input_bytes:
        add_kv("input", f"{_human_bytes(agg.input_bytes)} ({agg.input_records} records)")

    if agg.output_bytes:
        add_kv("output", f"{_human_bytes(agg.output_bytes)} ({agg.output_records} records)")

    # Time share vs executorRunTime (per-task time sum).
    if agg.executor_run_ms > 0:
        def pct(x: float) -> str:
            return f"{(100.0 * x / float(agg.executor_run_ms)):.1f}%"

        add_kv(
            "timeShare(execRun)",
            " / ".join([
                f"shuffleWrite={pct(agg.shuffle_write_ms())}",
                f"shuffleFetchWait={pct(agg.shuffle_read_fetch_wait_ms)}",
                f"gc={pct(agg.gc_ms)}",
                f"deserialize={pct(agg.executor_deserialize_ms)}",
                f"other~={pct(agg.est_other_ms())}",
            ]),
        )

    return "\n".join(parts)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Summarize Spark eventlog task metrics (shuffle read/write, spill, etc.)")
    p.add_argument("paths", nargs="+", help="Spark eventlog file(s) or a directory containing them")
    p.add_argument("--stages", default="", help="Comma-separated stage IDs to print (default: all)")
    args = p.parse_args(argv)

    stage_filter: Optional[set] = None
    if args.stages.strip():
        stage_filter = {int(x) for x in args.stages.split(",") if x.strip()}

    for path in _iter_eventlog_paths(args.paths):
        if not os.path.isfile(path):
            print(f"SKIP not a file: {path}", file=sys.stderr)
            continue

        props, app_start_ms, app_end_ms, stage_meta, stage_agg = _parse_eventlog(path)
        app_id = props.get("spark.app.id", "")
        app_name = props.get("spark.app.name", "")

        print(f"== {path} ==")
        if app_id or app_name:
            print(f"app: {app_name} ({app_id})".strip())
        if app_start_ms is not None and app_end_ms is not None and app_end_ms >= app_start_ms:
            print(f"appWall: {_human_ms(app_end_ms - app_start_ms)}")

        # Helpful toggles for IO interpretation.
        if "spark.shuffle.readHostLocalDisk" in props or "spark.shuffle.useOldFetchProtocol" in props:
            rhld = props.get("spark.shuffle.readHostLocalDisk", "<default>")
            old = props.get("spark.shuffle.useOldFetchProtocol", "<default>")
            print(f"conf: spark.shuffle.readHostLocalDisk={rhld}, spark.shuffle.useOldFetchProtocol={old}")

        # Print stages in numeric order.
        stage_ids = sorted(set(stage_meta.keys()) | set(stage_agg.keys()))
        for sid in stage_ids:
            if stage_filter is not None and sid not in stage_filter:
                continue
            meta = stage_meta.get(sid, StageMeta(stage_id=sid, attempt_id=0, name=""))
            agg = stage_agg.get(sid, StageAgg())
            print(_format_stage_line(meta, agg))

        # Totals.
        total = StageAgg()
        for sid in stage_ids:
            agg = stage_agg.get(sid)
            if agg is None:
                continue
            total.tasks += agg.tasks
            total.executor_run_ms += agg.executor_run_ms
            total.executor_deserialize_ms += agg.executor_deserialize_ms
            total.gc_ms += agg.gc_ms
            total.result_ser_ms += agg.result_ser_ms
            total.shuffle_read_fetch_wait_ms += agg.shuffle_read_fetch_wait_ms
            total.shuffle_read_remote_req_ms += agg.shuffle_read_remote_req_ms
            total.shuffle_read_remote_blocks += agg.shuffle_read_remote_blocks
            total.shuffle_read_local_blocks += agg.shuffle_read_local_blocks
            total.shuffle_read_remote_bytes += agg.shuffle_read_remote_bytes
            total.shuffle_read_remote_bytes_to_disk += agg.shuffle_read_remote_bytes_to_disk
            total.shuffle_read_local_bytes += agg.shuffle_read_local_bytes
            total.shuffle_read_records += agg.shuffle_read_records
            total.shuffle_write_bytes += agg.shuffle_write_bytes
            total.shuffle_write_records += agg.shuffle_write_records
            total.shuffle_write_time_ns += agg.shuffle_write_time_ns
            total.memory_spilled_bytes += agg.memory_spilled_bytes
            total.disk_spilled_bytes += agg.disk_spilled_bytes
            total.input_bytes += agg.input_bytes
            total.input_records += agg.input_records
            total.output_bytes += agg.output_bytes
            total.output_records += agg.output_records

        print("-- totals (sum over tasks; compare % vs execRun) --")
        print(_format_stage_line(StageMeta(stage_id=-1, attempt_id=0, name="TOTAL"), total))
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
