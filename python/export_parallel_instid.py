#!/usr/bin/env python3
"""Run one export worker per instId series with bounded concurrency."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

import extract_optiondata_to_parquet as exporter


SUPPORTED_MEASUREMENTS = {"okex_depth", "binance_depth20"}


@dataclass
class SeriesTask:
    measurement: str
    inst_id: str
    series_key: str
    start: str | None = None
    end: str | None = None
    window_label: str | None = None

    @property
    def task_id(self) -> str:
        if self.window_label:
            return f"{self.inst_id}@{self.window_label}"
        return self.inst_id


@dataclass
class WorkerState:
    task: SeriesTask
    process: subprocess.Popen[str]
    log_handle: TextIO
    log_path: Path
    dataset_path: Path
    started_at: float


@dataclass
class WorkerProgress:
    inst_id: str
    rows_written: int = 0
    buffer_rows: int = 0
    parts: int = 0
    range_text: str = "-..-"
    elapsed_seconds: int = 0
    source: str = "checkpoint"
    completed: bool = False


def safe_load_json(path: Path) -> dict[str, object] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export okex_depth or binance_depth20 concurrently by instId. "
            "Each worker exports exactly one instId series with its own checkpoint."
        )
    )
    parser.add_argument("measurement", choices=sorted(SUPPORTED_MEASUREMENTS))
    parser.add_argument("--host", default="127.0.0.1", help="InfluxDB host.")
    parser.add_argument("--port", type=int, default=8086, help="InfluxDB HTTP port.")
    parser.add_argument("--database", default="OptionData", help="InfluxDB database name.")
    parser.add_argument(
        "--output-dir",
        default="parallel_exports",
        help="Base output directory. Worker datasets are written under <output-dir>/<measurement>/.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Maximum concurrent instId workers. Default: 2.",
    )
    parser.add_argument(
        "--instid",
        action="append",
        dest="inst_ids",
        help="Optional instId to export. Repeat to export a subset only.",
    )
    parser.add_argument(
        "--list-instids",
        action="store_true",
        help="List discovered instIds for the measurement and exit.",
    )
    parser.add_argument(
        "--split",
        choices=("none", "monthly"),
        default="none",
        help=(
            "Task splitting mode. 'none' runs one worker per instId. "
            "'monthly' runs one worker per instId per month and requires --start and --end."
        ),
    )
    parser.add_argument(
        "--start",
        help="Inclusive RFC3339 UTC start timestamp, for example 2025-01-01T00:00:00Z.",
    )
    parser.add_argument(
        "--end",
        help="Exclusive RFC3339 UTC end timestamp, for example 2025-02-01T00:00:00Z.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional per-worker row limit for smoke tests.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="InfluxDB chunk size for streamed queries. Default: 1000.",
    )
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=50_000,
        help="Target rows per Parquet part file before flushing. Default: 50000.",
    )
    parser.add_argument(
        "--compression",
        default="zstd",
        help="Parquet compression codec. Default: zstd.",
    )
    parser.add_argument(
        "--compression-level",
        type=int,
        default=9,
        help="Compression level for codecs that support it. Default: 9.",
    )
    parser.add_argument("--username", help="Optional InfluxDB username.")
    parser.add_argument("--password", help="Optional InfluxDB password.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete any existing per-instId dataset before exporting.",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python interpreter used to launch worker processes. Default: current interpreter.",
    )
    return parser.parse_args()


def sanitize_name(value: str) -> str:
    safe = value.strip()
    for char in ('/', '\\', ':', '*', '?', '"', '<', '>', '|'):
        safe = safe.replace(char, "_")
    return safe or "series"


def parse_utc_timestamp(value: str) -> datetime:
    normalized = exporter.parse_rfc3339(value).replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).astimezone(UTC)


def utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def next_month_boundary(value: datetime) -> datetime:
    if value.month == 12:
        return value.replace(year=value.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return value.replace(month=value.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)


def compact_timestamp(value: str) -> str:
    return parse_utc_timestamp(value).strftime("%Y%m%dT%H%M%SZ")


def month_window_label(start: str, end: str) -> str:
    start_dt = parse_utc_timestamp(start)
    end_dt = parse_utc_timestamp(end)
    is_aligned_month = (
        start_dt.day == 1
        and start_dt.hour == 0
        and start_dt.minute == 0
        and start_dt.second == 0
        and start_dt.microsecond == 0
        and end_dt == next_month_boundary(start_dt)
    )
    if is_aligned_month:
        return start_dt.strftime("%Y-%m")
    return f"{compact_timestamp(start)}__{compact_timestamp(end)}"


def month_windows(start: str, end: str) -> list[tuple[str, str, str]]:
    start_dt = parse_utc_timestamp(start)
    end_dt = parse_utc_timestamp(end)
    if end_dt <= start_dt:
        raise RuntimeError("--end must be later than --start.")

    windows: list[tuple[str, str, str]] = []
    current_start = start_dt
    while current_start < end_dt:
        boundary = next_month_boundary(current_start)
        current_end = boundary if boundary < end_dt else end_dt
        start_iso = utc_iso(current_start)
        end_iso = utc_iso(current_end)
        windows.append((start_iso, end_iso, month_window_label(start_iso, end_iso)))
        current_start = current_end
    return windows


def discover_base_tasks(args: argparse.Namespace) -> list[SeriesTask]:
    client = exporter.InfluxClient(
        host=args.host,
        port=args.port,
        database=args.database,
        username=args.username,
        password=args.password,
    )
    series_keys = client.show_series_keys(args.measurement)
    tasks: list[SeriesTask] = []
    seen_inst_ids: set[str] = set()
    allowed_inst_ids = set(args.inst_ids or [])

    for series_key in series_keys:
        _, tags = exporter.parse_series_key(series_key)
        inst_id = tags.get("instId")
        if inst_id is None:
            continue
        if allowed_inst_ids and inst_id not in allowed_inst_ids:
            continue
        if inst_id in seen_inst_ids:
            raise RuntimeError(
                f"Measurement '{args.measurement}' has duplicate instId '{inst_id}' across multiple series."
            )
        seen_inst_ids.add(inst_id)
        tasks.append(SeriesTask(measurement=args.measurement, inst_id=inst_id, series_key=series_key))

    if allowed_inst_ids:
        missing = sorted(allowed_inst_ids - seen_inst_ids)
        if missing:
            raise RuntimeError(
                f"Requested instId values were not found in '{args.measurement}': {', '.join(missing)}"
            )

    return sorted(tasks, key=lambda task: task.inst_id)


def expand_tasks(args: argparse.Namespace, base_tasks: list[SeriesTask]) -> list[SeriesTask]:
    if args.split == "none":
        return base_tasks

    if args.start is None or args.end is None:
        raise RuntimeError("--split monthly requires both --start and --end.")

    windows = month_windows(args.start, args.end)
    tasks: list[SeriesTask] = []
    for base_task in base_tasks:
        for start, end, window_label in windows:
            tasks.append(
                SeriesTask(
                    measurement=base_task.measurement,
                    inst_id=base_task.inst_id,
                    series_key=base_task.series_key,
                    start=start,
                    end=end,
                    window_label=window_label,
                )
            )
    # For large measurements like okex_depth, launching adjacent windows from the
    # same instId can pin InfluxDB on one hot series before any worker produces
    # its first chunk. Month-first ordering spreads the first wave across
    # different instIds and usually yields earlier visible progress.
    return sorted(tasks, key=lambda task: (task.start or "", task.end or "", task.inst_id))


def build_worker_command(
    args: argparse.Namespace,
    task: SeriesTask,
    measurement_output_dir: Path,
) -> tuple[list[str], Path]:
    if task.window_label is None:
        worker_output_dir = measurement_output_dir
        dataset_name = sanitize_name(task.inst_id)
    else:
        worker_output_dir = measurement_output_dir / f"instId={sanitize_name(task.inst_id)}"
        dataset_name = sanitize_name(task.window_label)
    dataset_path = worker_output_dir / f"{dataset_name}.parquet"
    script_path = Path(__file__).with_name("extract_optiondata_to_parquet.py")
    command = [
        args.python,
        str(script_path),
        task.measurement,
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--database",
        args.database,
        "--output-dir",
        str(worker_output_dir),
        "--dataset-name",
        dataset_name,
        "--series-key",
        task.series_key,
        "--chunk-size",
        str(args.chunk_size),
        "--rows-per-file",
        str(args.rows_per_file),
        "--compression",
        args.compression,
        "--compression-level",
        str(args.compression_level),
    ]
    task_start = task.start or args.start
    task_end = task.end or args.end
    if task_start:
        command.extend(["--start", task_start])
    if task_end:
        command.extend(["--end", task_end])
    if args.limit is not None:
        command.extend(["--limit", str(args.limit)])
    if args.username:
        command.extend(["--username", args.username])
    if args.password:
        command.extend(["--password", args.password])
    if args.overwrite:
        command.append("--overwrite")
    return command, dataset_path


def launch_worker(
    args: argparse.Namespace,
    task: SeriesTask,
    measurement_output_dir: Path,
    logs_dir: Path,
) -> WorkerState:
    command, dataset_path = build_worker_command(args, task, measurement_output_dir)
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"{sanitize_name(task.task_id)}.log"
    log_handle = log_path.open("a", encoding="utf-8", buffering=1)
    process = subprocess.Popen(
        command,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return WorkerState(
        task=task,
        process=process,
        log_handle=log_handle,
        log_path=log_path,
        dataset_path=dataset_path,
        started_at=time.monotonic(),
    )


def write_manifest(path: Path, measurement: str, tasks: list[SeriesTask], completed: dict[str, dict[str, object]]) -> None:
    payload = {
        "measurement": measurement,
        "generated_at": exporter.utc_now_iso(),
        "tasks": [
            {
                "task_id": task.task_id,
                "inst_id": task.inst_id,
                "series_key": task.series_key,
                "start": task.start,
                "end": task.end,
                "window_label": task.window_label,
                "status": completed.get(task.task_id, {}).get("status", "pending"),
                "exit_code": completed.get(task.task_id, {}).get("exit_code"),
                "dataset_path": completed.get(task.task_id, {}).get("dataset_path"),
                "log_path": completed.get(task.task_id, {}).get("log_path"),
            }
            for task in tasks
        ],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def checkpoint_int(checkpoint: dict[str, object], key: str) -> int | None:
    value = checkpoint.get(key)
    if isinstance(value, int):
        return value
    return None


PROGRESS_PATTERN = re.compile(
    r"rows_written=(?P<rows_written>\d+).*?"
    r"buffer_rows=(?P<buffer_rows>\d+).*?"
    r"parts=(?P<parts>\d+).*?"
    r"range=(?P<range>\S+).*?"
    r"elapsed=(?P<elapsed>\d+)s"
)


def tail_text(path: Path, max_bytes: int = 65536) -> str:
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes))
            return handle.read().decode("utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def latest_progress_from_log(log_path: Path, inst_id: str) -> WorkerProgress | None:
    text = tail_text(log_path)
    for line in reversed(text.splitlines()):
        if not line.startswith("[progress] "):
            continue
        match = PROGRESS_PATTERN.search(line)
        if not match:
            continue
        return WorkerProgress(
            inst_id=inst_id,
            rows_written=int(match.group("rows_written")),
            buffer_rows=int(match.group("buffer_rows")),
            parts=int(match.group("parts")),
            range_text=match.group("range"),
            elapsed_seconds=int(match.group("elapsed")),
            source="log",
        )
    return None


def worker_progress(worker: WorkerState) -> WorkerProgress:
    log_progress = latest_progress_from_log(worker.log_path, worker.task.inst_id)
    if log_progress is not None:
        return log_progress


    checkpoint = safe_load_json(worker.dataset_path / "_checkpoint.json")
    if checkpoint is None:
        return WorkerProgress(
            inst_id=worker.task.inst_id,
            elapsed_seconds=int(time.monotonic() - worker.started_at),
            source="starting",
        )

    rows_written = checkpoint_int(checkpoint, "rows_written") or 0
    part_index = checkpoint_int(checkpoint, "part_index") or 0
    min_time = exporter.ns_to_iso8601(checkpoint_int(checkpoint, "min_time_ns"))
    max_time = exporter.ns_to_iso8601(checkpoint_int(checkpoint, "max_time_ns"))
    completed = bool(checkpoint.get("completed", False))
    return WorkerProgress(
        inst_id=worker.task.inst_id,
        rows_written=rows_written,
        buffer_rows=0,
        parts=part_index,
        range_text=f"{min_time or '-'}..{max_time or '-'}",
        elapsed_seconds=int(time.monotonic() - worker.started_at),
        source="checkpoint",
        completed=completed,
    )


def print_worker_progress(worker: WorkerState, rows_per_file: int) -> WorkerProgress:
    progress = worker_progress(worker)
    if rows_per_file > 0:
        buffer_pct = (progress.buffer_rows / rows_per_file) * 100
        buffer_text = f"{progress.buffer_rows}/{rows_per_file} ({buffer_pct:.1f}%)"
    else:
        buffer_text = str(progress.buffer_rows)
    print(
        "[worker] "
        f"task={worker.task.task_id} "
        f"rows={progress.rows_written} "
        f"buffer={buffer_text} "
        f"parts={progress.parts} "
        f"range={progress.range_text} "
        f"elapsed={progress.elapsed_seconds}s",
        flush=True,
    )
    return progress


def print_runner_progress(
    measurement: str,
    queued: int,
    running: dict[str, WorkerState],
    completed: dict[str, dict[str, object]],
    started_at: float,
    rows_per_file: int,
) -> None:
    finished = sum(1 for item in completed.values() if item["exit_code"] == 0)
    failed = sum(1 for item in completed.values() if item["exit_code"] != 0)
    progress_items = [worker_progress(running[inst_id]) for inst_id in sorted(running)]
    total_rows_written = sum(item.rows_written for item in progress_items)
    total_buffer_rows = sum(item.buffer_rows for item in progress_items)
    total_parts = sum(item.parts for item in progress_items)
    print(
        "[runner] "
        f"measurement={measurement} "
        f"queued={queued} running={len(running)} finished={finished} failed={failed} "
        f"rows={total_rows_written} buffer={total_buffer_rows} parts={total_parts} "
        f"elapsed={int(time.monotonic() - started_at)}s",
        flush=True,
    )
    for inst_id in sorted(running):
        print_worker_progress(running[inst_id], rows_per_file)


def main() -> int:
    args = parse_args()
    base_tasks = discover_base_tasks(args)

    if args.list_instids:
        for task in base_tasks:
            print(task.inst_id)
        return 0

    tasks = expand_tasks(args, base_tasks)

    if not tasks:
        print(f"No instId series found for measurement '{args.measurement}'.", file=sys.stderr)
        return 1

    base_output_dir = Path(args.output_dir).resolve()
    measurement_output_dir = base_output_dir / args.measurement
    measurement_output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = measurement_output_dir / "_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = measurement_output_dir / "_manifest.json"

    print(
        "[start] "
        f"measurement={args.measurement} workers={args.workers} "
        f"split={args.split} tasks={len(tasks)} inst_ids={len(base_tasks)} "
        f"output_dir={measurement_output_dir}",
        flush=True,
    )

    pending = list(tasks)
    running: dict[str, WorkerState] = {}
    completed: dict[str, dict[str, object]] = {}
    started_at = time.monotonic()
    last_progress = 0.0

    try:
        while pending or running:
            while pending and len(running) < args.workers:
                task = pending.pop(0)
                worker = launch_worker(args, task, measurement_output_dir, logs_dir)
                running[task.task_id] = worker
                print(
                    "[launch] "
                    f"measurement={task.measurement} task={task.task_id} "
                    f"dataset={worker.dataset_path} log={worker.log_path}",
                    flush=True,
                )

            time.sleep(1)

            finished_task_ids: list[str] = []
            for task_id, worker in running.items():
                exit_code = worker.process.poll()
                if exit_code is None:
                    continue
                finished_task_ids.append(task_id)
                worker.log_handle.close()
                completed[task_id] = {
                    "status": "completed" if exit_code == 0 else "failed",
                    "exit_code": exit_code,
                    "dataset_path": str(worker.dataset_path),
                    "log_path": str(worker.log_path),
                }
                print(
                    "[worker-done] "
                    f"task={task_id} exit_code={exit_code} "
                    f"dataset={worker.dataset_path} log={worker.log_path}",
                    flush=True,
                )

            for task_id in finished_task_ids:
                running.pop(task_id, None)

            now = time.monotonic()
            if now - last_progress >= 5:
                write_manifest(manifest_path, args.measurement, tasks, completed)
                print_runner_progress(
                    args.measurement,
                    len(pending),
                    running,
                    completed,
                    started_at,
                    args.rows_per_file,
                )
                last_progress = now

    except KeyboardInterrupt:
        print("[interrupt] terminating worker processes...", flush=True)
        for worker in running.values():
            worker.process.terminate()
        for worker in running.values():
            worker.process.wait(timeout=10)
            worker.log_handle.close()
        write_manifest(manifest_path, args.measurement, tasks, completed)
        return 130

    write_manifest(manifest_path, args.measurement, tasks, completed)
    failures = [task_id for task_id, item in completed.items() if item["exit_code"] != 0]
    if failures:
        print(
            "[done] "
            f"measurement={args.measurement} status=failed failed_tasks={','.join(sorted(failures))}",
            flush=True,
        )
        return 1

    print(
        "[done] "
        f"measurement={args.measurement} status=completed tasks={len(tasks)} "
        f"output_dir={measurement_output_dir}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
