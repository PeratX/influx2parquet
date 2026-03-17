#!/usr/bin/env python3
"""Export okex_depth directly from TSM files with a resumable 4-phase pipeline.

Pipeline phases:

1. scan
   Walk all TSM files under either <data-dir>/<database>/<retention>/ or
   <database-root>/<retention>/ and build a local index of which files contain
   okex_depth keys.
2. export
   Run `influx_inspect export -tsmfile ...` only for matching files and spool
   raw per-file/per-instId/per-field TSV fragments.
3. merge
   Merge and deduplicate raw field fragments into one TSV per instId/field.
4. build
   Reconstruct rows from merged field TSVs and write the final Parquet datasets:

       <output-dir>/okex_depth/
         BTC-USDT-SWAP.parquet/
         DOGE-USDT-SWAP.parquet/
         ...

Every phase is resumable. Re-running the same command continues from the last
completed phase/task unless --overwrite is used.

Run this script as a user that can read the InfluxDB TSM tree, typically:

    sudo -u influxdb /home/niko/influx2parquet/.venv/bin/python export_okex_depth_tsm.py ...
"""

from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
import heapq
import json
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Iterator

import extract_optiondata_to_parquet as exporter
import zstandard as zstd

try:
    import pyarrow as pa
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: pyarrow. Install it in the active environment before running this script."
    ) from exc


MEASUREMENT = "okex_depth"
DEFAULT_START = "2022-05-26T08:50:23.185000000Z"
DEFAULT_END = "2023-11-21T04:31:03.229056001Z"
PIPELINE_VERSION = 2
OKEX_DEPTH_FIELD_TYPES = {
    "action": "string",
    "asks": "string",
    "bids": "string",
    "checksum": "integer",
    "ts": "string",
}
OKEX_DEPTH_TAG_KEYS = ["instId"]
OKEX_DEPTH_FIELDS = ["action", "asks", "bids", "checksum", "ts"]
TSV_ZST_SUFFIX = ".tsv.zst"
TSV_PLAIN_SUFFIX = ".tsv"
INST_ID_RE = re.compile(r"okex_depth,instId=([^,\s]+)")
TSM_NAME_RE = re.compile(r"(?P<generation>\d+)-(?P<sequence>\d+)\.tsm$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export okex_depth directly from TSM files with scan/export/merge/build phases. "
            "No InfluxDB HTTP query path is used."
        )
    )
    parser.add_argument("--database", default="OptionData", help="InfluxDB database name.")
    parser.add_argument("--retention", default="autogen", help="Retention policy name.")
    parser.add_argument(
        "--data-dir",
        default="/opt/my_influx/data",
        help="InfluxDB data root or database root.",
    )
    parser.add_argument(
        "--wal-dir",
        default="/opt/my_influx/wal",
        help="InfluxDB WAL root or database root. Passed through to influx_inspect export.",
    )
    parser.add_argument(
        "--inspect-binary",
        default="influx_inspect",
        help="Path to influx_inspect. Default: influx_inspect from PATH.",
    )
    parser.add_argument(
        "--output-dir",
        default="direct_exports",
        help="Base output directory. Final datasets are written under <output-dir>/okex_depth/.",
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START,
        help=f"Inclusive RFC3339 UTC start timestamp. Default: {DEFAULT_START}",
    )
    parser.add_argument(
        "--end",
        default=DEFAULT_END,
        help=f"Exclusive RFC3339 UTC end timestamp. Default: {DEFAULT_END}",
    )
    parser.add_argument(
        "--instid",
        action="append",
        dest="inst_ids",
        help="Optional instId to export. Repeat to export a subset only.",
    )
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=5000,
        help="Target rows per Parquet part before flushing. Default: 5000.",
    )
    parser.add_argument(
        "--compression",
        default="zstd",
        help="Parquet compression codec. Default: zstd.",
    )
    parser.add_argument(
        "--compression-level",
        type=int,
        default=5,
        help="Compression level for codecs that support it. Default: 5.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel workers for scan/export/merge tasks. Default: 1.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=5,
        help="Print file-export progress every N seconds. Default: 5.",
    )
    parser.add_argument(
        "--export-queue-bytes",
        type=int,
        default=64 * 1024 * 1024,
        help=(
            "Max buffered stdout bytes per export worker before backpressure. "
            "Default: 67108864 (64 MiB)."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete any existing okex_depth output tree and restart from scratch.",
    )
    parser.add_argument(
        "--stop-after",
        choices=["scan", "export", "merge", "build"],
        help="Stop after a given phase. Useful for staged execution and debugging.",
    )
    return parser.parse_args()


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(exporter.parse_rfc3339(value).replace("Z", "+00:00")).astimezone(UTC)


def utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def safe_load_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return None


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def human_bytes(value: int) -> str:
    return exporter.human_bytes(value)


def human_duration(seconds: float) -> str:
    whole = max(0, int(seconds))
    minutes, secs = divmod(whole, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def measurement_root(output_dir: str) -> Path:
    return Path(output_dir).resolve() / MEASUREMENT


def pipeline_state_path(root: Path) -> Path:
    return root / "_pipeline_state.json"


def tsm_index_path(root: Path) -> Path:
    return root / "_tsm_index.json"


def raw_root(root: Path) -> Path:
    return root / "_raw"


def merged_root(root: Path) -> Path:
    return root / "_merged"


def build_root(root: Path) -> Path:
    return root / "_build"


def raw_manifest_path(root: Path, file_id: str) -> Path:
    return raw_root(root) / file_id / "_manifest.json"


def compressed_field_path(base_dir: Path, field_name: str) -> Path:
    return base_dir / f"{field_name}{TSV_ZST_SUFFIX}"


def legacy_field_path(base_dir: Path, field_name: str) -> Path:
    return base_dir / f"{field_name}{TSV_PLAIN_SUFFIX}"


def resolve_existing_field_path(base_dir: Path, field_name: str) -> Path | None:
    for candidate in (compressed_field_path(base_dir, field_name), legacy_field_path(base_dir, field_name)):
        if candidate.exists():
            return candidate
    return None


def merged_field_path(root: Path, inst_id: str, field_name: str) -> Path:
    return compressed_field_path(merged_root(root) / inst_id, field_name)


@contextmanager
def open_spool_text_writer(path: Path, compression_level: int = 1) -> Iterator[Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".zst":
        cctx = zstd.ZstdCompressor(level=compression_level)
        with zstd.open(path, mode="wt", encoding="utf-8", cctx=cctx) as handle:
            yield handle
        return
    with path.open("w", encoding="utf-8") as handle:
        yield handle


@contextmanager
def open_spool_text_reader(path: Path) -> Iterator[Any]:
    if path.suffix == ".zst":
        with zstd.open(path, mode="rt", encoding="utf-8") as handle:
            yield handle
        return
    with path.open("r", encoding="utf-8") as handle:
        yield handle


def build_stage_dataset_path(root: Path, inst_id: str) -> Path:
    return build_root(root) / f"{inst_id}.parquet"


def lp_unescape(text: str) -> str:
    chars: list[str] = []
    escaped = False
    for char in text:
        if escaped:
            chars.append(char)
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        chars.append(char)
    if escaped:
        chars.append("\\")
    return "".join(chars)


def lp_decode_value(raw_value: str) -> Any:
    if raw_value.startswith('"') and raw_value.endswith('"'):
        return lp_unescape(raw_value[1:-1])
    if raw_value.endswith("i"):
        return int(raw_value[:-1])
    if raw_value in {"t", "T", "true", "TRUE"}:
        return True
    if raw_value in {"f", "F", "false", "FALSE"}:
        return False
    try:
        return float(raw_value)
    except ValueError:
        return raw_value


def split_lp_outside_quotes(text: str, separator: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    escaped = False
    in_quotes = False
    for char in text:
        if escaped:
            current.append(char)
            escaped = False
            continue
        if char == "\\":
            current.append(char)
            escaped = True
            continue
        if char == '"':
            current.append(char)
            in_quotes = not in_quotes
            continue
        if char == separator and not in_quotes:
            parts.append("".join(current))
            current = []
            continue
        current.append(char)
    if escaped:
        current.append("\\")
    parts.append("".join(current))
    return parts


def split_first_lp_space(text: str) -> tuple[str, str] | None:
    escaped = False
    in_quotes = False
    for index, char in enumerate(text):
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == '"':
            in_quotes = not in_quotes
            continue
        if char == " " and not in_quotes:
            return text[:index], text[index + 1 :]
    return None


def split_last_lp_space(text: str) -> tuple[str, str] | None:
    escaped = False
    in_quotes = False
    for index in range(len(text) - 1, -1, -1):
        char = text[index]
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == '"':
            in_quotes = not in_quotes
            continue
        if char == " " and not in_quotes:
            return text[:index], text[index + 1 :]
    return None


def parse_okex_depth_lp_line(line: str) -> tuple[str, dict[str, Any], int] | None:
    text = line.rstrip("\r\n")
    marker = f"{MEASUREMENT},"
    marker_index = text.find(marker)
    if marker_index == -1:
        return None
    if marker_index > 0:
        text = text[marker_index:]

    head_split = split_first_lp_space(text)
    if head_split is None:
        return None
    series_key, fields_and_time = head_split

    tail_split = split_last_lp_space(fields_and_time)
    if tail_split is None:
        return None
    fields_text, timestamp_text = tail_split

    measurement, tags = exporter.parse_series_key(series_key)
    if measurement != MEASUREMENT:
        return None
    inst_id = tags.get("instId")
    if not inst_id:
        return None

    try:
        timestamp_ns = int(timestamp_text)
    except ValueError:
        return None

    fields: dict[str, Any] = {}
    for raw_field in split_lp_outside_quotes(fields_text, ","):
        if not raw_field:
            continue
        key, raw_value = exporter.split_first_escaped(raw_field, "=")
        if not key or not raw_value:
            continue
        field_name = lp_unescape(key)
        fields[field_name] = lp_decode_value(raw_value)

    if not fields:
        return None
    return inst_id, fields, timestamp_ns


def append_spool_line(handle: Any, timestamp_ns: int, value: Any) -> None:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    handle.write(f"{timestamp_ns}\t{encoded}\n")


def decode_spool_value(raw_value: str) -> Any:
    return json.loads(raw_value)


def tsv_iter(path: Path) -> Iterator[tuple[int, Any]]:
    with open_spool_text_reader(path) as handle:
        for raw_line in handle:
            raw_line = raw_line.rstrip("\n")
            if not raw_line:
                continue
            ts_text, raw_value = raw_line.split("\t", 1)
            yield int(ts_text), decode_spool_value(raw_value)


def consume_same_timestamp(
    current: tuple[int, Any] | None,
    iterator: Iterator[tuple[int, Any]],
) -> tuple[Any | None, tuple[int, Any] | None]:
    if current is None:
        return None, None
    timestamp, value = current
    latest = value
    next_item: tuple[int, Any] | None = None
    while True:
        try:
            candidate = next(iterator)
        except StopIteration:
            next_item = None
            break
        if candidate[0] != timestamp:
            next_item = candidate
            break
        latest = candidate[1]
    return latest, next_item


def collapsed_tsv_iter(path: Path) -> Iterator[tuple[int, Any]]:
    iterator = tsv_iter(path)
    current = next(iterator, None)
    while current is not None:
        timestamp = current[0]
        value, current = consume_same_timestamp(current, iterator)
        if value is not None:
            yield timestamp, value


def table_from_rows(schema: pa.Schema, rows: list[dict[str, Any]]) -> pa.Table:
    arrays: list[pa.Array] = []
    for field in schema:
        values = [row.get(field.name) for row in rows]
        arrays.append(exporter.convert_to_array(values, field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def parse_tsm_name(file_name: str) -> tuple[int, int]:
    match = TSM_NAME_RE.match(file_name)
    if match is None:
        return (0, 0)
    return (int(match.group("generation")), int(match.group("sequence")))


@dataclass(order=True)
class TsmFile:
    shard_id: int
    generation: int
    sequence: int
    rel_path: str
    abs_path: str
    size_bytes: int

    @property
    def file_id(self) -> str:
        shard, file_name = self.rel_path.split("/", 1)
        stem = file_name.removesuffix(".tsm")
        return f"shard-{shard}__{stem}"


@dataclass(frozen=True)
class InfluxTree:
    role: str
    input_root: str
    database_root: str
    retention_root: str
    layout: str


@dataclass
class ScanResult:
    file_id: str
    rel_path: str
    abs_path: str
    shard_id: int
    size_bytes: int
    generation: int
    sequence: int
    has_measurement: bool
    inst_ids: list[str]
    elapsed_seconds: float


@dataclass
class ExportResult:
    file_id: str
    rel_path: str
    shard_id: int
    generation: int
    sequence: int
    raw_dir: str
    stream_bytes: int
    matched_lines: int
    field_line_counts: dict[str, dict[str, int]]
    inst_ids: list[str]
    min_time_ns: int | None
    max_time_ns: int | None
    elapsed_seconds: float


@dataclass
class MergeResult:
    inst_id: str
    field_name: str
    dest_path: str
    row_count: int
    source_count: int
    source_files: list[str]
    elapsed_seconds: float


@dataclass
class DatasetBuildStats:
    rows: int = 0
    part_count: int = 0
    bytes_written: int = 0
    min_time_ns: int | None = None
    max_time_ns: int | None = None


@dataclass
class BuildResult:
    inst_id: str
    stage_dataset_path: str
    rows: int
    part_count: int
    bytes_written: int
    min_time_ns: int | None
    max_time_ns: int | None
    merged_signature: dict[str, dict[str, Any]]
    elapsed_seconds: float


@dataclass
class ExportRuntimeProgress:
    file_id: str
    rel_path: str
    progress_interval: int
    bytes_read: int = 0
    matched_lines: int = 0
    last_inst_id: str | None = None
    last_timestamp_ns: int | None = None
    started_at: float = field(default_factory=time.monotonic)
    last_print_at: float = field(default_factory=time.monotonic)

    def note_line(
        self,
        raw_line: str,
        inst_id: str | None,
        timestamp_ns: int | None,
        size_bytes: int | None = None,
    ) -> None:
        self.bytes_read += size_bytes if size_bytes is not None else len(raw_line.encode("utf-8", errors="replace"))
        if inst_id is not None:
            self.matched_lines += 1
            self.last_inst_id = inst_id
            self.last_timestamp_ns = timestamp_ns
        self.maybe_print()

    def maybe_print(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_print_at < self.progress_interval:
            return
        self.last_print_at = now
        last_time = exporter.ns_to_iso8601(self.last_timestamp_ns) or "-"
        print(
            "[export-file] "
            f"file={self.file_id} "
            f"rel={self.rel_path} "
            f"stream={human_bytes(self.bytes_read)} "
            f"lines={self.matched_lines} "
            f"last_inst={self.last_inst_id or '-'} "
            f"last_time={last_time} "
            f"elapsed={human_duration(now - self.started_at)}",
            flush=True,
        )


@dataclass
class ScanPhaseProgress:
    total_files: int
    total_bytes: int
    progress_interval: int
    completed_files: int = 0
    completed_bytes: int = 0
    matched_files: int = 0
    matched_bytes: int = 0
    started_at: float = field(default_factory=time.monotonic)
    last_print_at: float = field(default_factory=time.monotonic)

    def note_completed(self, size_bytes: int, matched: bool) -> None:
        self.completed_files += 1
        self.completed_bytes += size_bytes
        if matched:
            self.matched_files += 1
            self.matched_bytes += size_bytes
        self.maybe_print(force=True)

    def maybe_print(self, force: bool = False, running: int = 0) -> None:
        now = time.monotonic()
        if not force and now - self.last_print_at < self.progress_interval:
            return
        self.last_print_at = now
        print(
            "[scan-progress] "
            f"done={self.completed_files}/{self.total_files} "
            f"running={running} "
            f"scanned={human_bytes(self.completed_bytes)}/{human_bytes(self.total_bytes)} "
            f"matched={self.matched_files} "
            f"matched_size={human_bytes(self.matched_bytes)} "
            f"elapsed={human_duration(now - self.started_at)}",
            flush=True,
        )


@dataclass
class MergeRuntimeProgress:
    inst_id: str
    field_name: str
    source_count: int
    progress_interval: int
    rows_written: int = 0
    last_timestamp_ns: int | None = None
    started_at: float = field(default_factory=time.monotonic)
    last_print_at: float = field(default_factory=time.monotonic)

    def note_row(self, timestamp_ns: int) -> None:
        self.rows_written += 1
        self.last_timestamp_ns = timestamp_ns
        self.maybe_print()

    def maybe_print(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_print_at < self.progress_interval:
            return
        self.last_print_at = now
        print(
            "[merge-progress] "
            f"instId={self.inst_id} "
            f"field={self.field_name} "
            f"rows={self.rows_written} "
            f"sources={self.source_count} "
            f"last_time={exporter.ns_to_iso8601(self.last_timestamp_ns) or '-'} "
            f"elapsed={human_duration(now - self.started_at)}",
            flush=True,
        )


@dataclass
class BuildRuntimeProgress:
    inst_id: str
    progress_interval: int
    rows_written: int = 0
    parts_written: int = 0
    bytes_written: int = 0
    last_timestamp_ns: int | None = None
    started_at: float = field(default_factory=time.monotonic)
    last_print_at: float = field(default_factory=time.monotonic)

    def note_row(self, timestamp_ns: int) -> None:
        self.rows_written += 1
        self.last_timestamp_ns = timestamp_ns
        self.maybe_print()

    def note_flush(self, part_count: int, bytes_written: int) -> None:
        self.parts_written = part_count
        self.bytes_written = bytes_written
        self.maybe_print(force=True)

    def maybe_print(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_print_at < self.progress_interval:
            return
        self.last_print_at = now
        print(
            "[build-progress] "
            f"instId={self.inst_id} "
            f"rows={self.rows_written} "
            f"parts={self.parts_written} "
            f"size={human_bytes(self.bytes_written)} "
            f"last_time={exporter.ns_to_iso8601(self.last_timestamp_ns) or '-'} "
            f"elapsed={human_duration(now - self.started_at)}",
            flush=True,
        )


class BoundedTextQueue:
    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max(1, int(max_bytes))
        self._items: deque[tuple[str, int]] = deque()
        self._queued_bytes = 0
        self._closed = False
        self._condition = threading.Condition()

    def put(self, text: str) -> bool:
        size_bytes = len(text.encode("utf-8", errors="replace"))
        with self._condition:
            while True:
                if self._closed:
                    return False
                if self._queued_bytes + size_bytes <= self.max_bytes or not self._items:
                    self._items.append((text, size_bytes))
                    self._queued_bytes += size_bytes
                    self._condition.notify_all()
                    return True
                self._condition.wait()

    def get(self) -> tuple[str, int] | None:
        with self._condition:
            while not self._items:
                if self._closed:
                    return None
                self._condition.wait()
            item = self._items.popleft()
            self._queued_bytes -= item[1]
            self._condition.notify_all()
            return item

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._condition.notify_all()


def pump_process_stdout(
    stdout: Any,
    line_queue: BoundedTextQueue,
    error_holder: dict[str, BaseException],
) -> None:
    try:
        with stdout:
            for raw_line in stdout:
                if not line_queue.put(raw_line):
                    return
    except BaseException as exc:  # pragma: no cover - thread error propagation
        error_holder["error"] = exc
    finally:
        line_queue.close()


class ParquetDatasetBuilder:
    def __init__(
        self,
        schema: pa.Schema,
        dataset_path: Path,
        rows_per_file: int,
        compression: str,
        compression_level: int,
        progress: BuildRuntimeProgress | None = None,
    ) -> None:
        self.schema = schema
        self.dataset_path = dataset_path
        self.rows_per_file = rows_per_file
        self.compression = compression
        self.compression_level = compression_level
        self.progress = progress
        self.dataset_path.mkdir(parents=True, exist_ok=True)
        self.buffer_rows: list[dict[str, Any]] = []
        self.stats = DatasetBuildStats()

    def append_row(self, row: dict[str, Any]) -> None:
        timestamp_ns = row["time"]
        self.buffer_rows.append(row)
        self.stats.rows += 1
        if self.stats.min_time_ns is None or timestamp_ns < self.stats.min_time_ns:
            self.stats.min_time_ns = timestamp_ns
        if self.stats.max_time_ns is None or timestamp_ns > self.stats.max_time_ns:
            self.stats.max_time_ns = timestamp_ns
        if self.progress is not None:
            self.progress.note_row(timestamp_ns)
        if len(self.buffer_rows) >= self.rows_per_file:
            self.flush()

    def flush(self) -> None:
        if not self.buffer_rows:
            return
        table = table_from_rows(self.schema, self.buffer_rows)
        _, file_size = exporter.write_part_file(
            dataset_path=self.dataset_path,
            part_index=self.stats.part_count,
            schema=self.schema,
            tables=[table],
            compression=self.compression,
            compression_level=self.compression_level,
        )
        self.stats.part_count += 1
        self.stats.bytes_written += file_size
        self.buffer_rows = []
        if self.progress is not None:
            self.progress.note_flush(self.stats.part_count, self.stats.bytes_written)

    def finalize(self) -> DatasetBuildStats:
        self.flush()
        return self.stats


def source_command_template(args: argparse.Namespace, data_tree: InfluxTree, wal_tree: InfluxTree) -> str:
    return (
        f"{args.inspect_binary} export "
        f"-database {args.database} "
        f"-retention {args.retention} "
        f"-datadir {data_tree.input_root} "
        f"-waldir {wal_tree.input_root} "
        f"-tsmfile <tsm-file> "
        f"-start {args.start} "
        f"-end {args.end} "
        f"-lponly -out -"
    )


def initialize_final_checkpoint(
    dataset_path: Path,
    inst_id: str,
    database: str,
    start: str,
    end: str,
    rows_per_file: int,
    compression: str,
    compression_level: int,
) -> exporter.Checkpoint:
    return exporter.Checkpoint(
        measurement=MEASUREMENT,
        database=database,
        dataset_path=str(dataset_path),
        start=start,
        end=end,
        limit=None,
        chunk_size=0,
        rows_per_file=rows_per_file,
        compression=compression,
        compression_level=compression_level,
        series_key=f"{MEASUREMENT},instId={inst_id}",
        rows_per_file_history=[rows_per_file],
        completed_series=[],
        completed=False,
    )


def write_dataset_metadata(
    dataset_path: Path,
    checkpoint: exporter.Checkpoint,
    inst_id: str,
    schema: pa.Schema,
    command_template: str,
) -> None:
    description_rows = exporter.schema_description_rows(schema, OKEX_DEPTH_FIELD_TYPES, set(OKEX_DEPTH_TAG_KEYS))
    rows_per_file_history = checkpoint.rows_per_file_history or [checkpoint.rows_per_file]
    summary = {
        "measurement": MEASUREMENT,
        "dataset_path": str(dataset_path),
        "inst_id": inst_id,
        "record_count": checkpoint.rows_written,
        "date_range": {
            "min_time_ns": checkpoint.min_time_ns,
            "max_time_ns": checkpoint.max_time_ns,
            "min_time": exporter.ns_to_iso8601(checkpoint.min_time_ns),
            "max_time": exporter.ns_to_iso8601(checkpoint.max_time_ns),
        },
        "compression": {
            "codec": checkpoint.compression,
            "level": checkpoint.compression_level,
        },
        "part_count": checkpoint.part_index,
        "dataset_size_bytes": checkpoint.bytes_written,
        "dataset_size_human": human_bytes(checkpoint.bytes_written),
        "series_count": 1,
        "rows_per_file_target": checkpoint.rows_per_file,
        "rows_per_file_history": rows_per_file_history,
        "source_command_template": command_template,
        "table_format": description_rows,
    }
    exporter.summary_path(dataset_path).write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    lines = [
        f"# Measurement Export Description: {MEASUREMENT}",
        "",
        "## Overview",
        f"- Dataset path: `{dataset_path}`",
        f"- instId: `{inst_id}`",
        f"- Dataset size: `{human_bytes(checkpoint.bytes_written)}`",
        f"- Part files: `{checkpoint.part_index}`",
        f"- Record count: `{checkpoint.rows_written}`",
        f"- Date range: `{exporter.ns_to_iso8601(checkpoint.min_time_ns) or 'n/a'}` to `{exporter.ns_to_iso8601(checkpoint.max_time_ns) or 'n/a'}`",
        f"- Compression: `{checkpoint.compression}` level `{checkpoint.compression_level}`",
        f"- Target rows per part: `{checkpoint.rows_per_file}`",
    ]
    if len(rows_per_file_history) > 1:
        lines.append(f"- Rows per part history: `{ ' -> '.join(str(item) for item in rows_per_file_history) }`")
    lines.extend(
        [
            "",
            "## Source Command Template",
            "```bash",
            command_template,
            "```",
            "",
            "## Table Format",
            "| Column | Kind | Influx Type | Parquet Type |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in description_rows:
        lines.append(f"| {row['name']} | {row['kind']} | {row['influx_type']} | {row['parquet_type']} |")
    exporter.description_path(dataset_path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def checkpoint_exists(dataset_path: Path) -> bool:
    return exporter.checkpoint_path(dataset_path).exists()


def export_artifact_complete(root: Path, file_id: str) -> bool:
    manifest = safe_load_json(raw_manifest_path(root, file_id))
    if manifest is None:
        return False
    raw_dir_value = manifest.get("raw_dir")
    if not raw_dir_value:
        return False
    raw_dir = Path(raw_dir_value)
    if not raw_dir.exists():
        return False
    for inst_id, field_counts in manifest.get("field_line_counts", {}).items():
        for field_name, count in field_counts.items():
            if count <= 0:
                continue
            if resolve_existing_field_path(raw_dir / inst_id, field_name) is None:
                return False
    return True


def ensure_clean_root(root: Path, overwrite: bool) -> None:
    if overwrite and root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)


def resolve_influx_tree(root_dir: Path, database: str, retention: str, role: str) -> InfluxTree:
    input_root = root_dir.resolve()
    data_root_retention = input_root / database / retention
    database_root_retention = input_root / retention

    if data_root_retention.is_dir():
        database_root = input_root / database
        return InfluxTree(
            role=role,
            input_root=str(input_root),
            database_root=str(database_root),
            retention_root=str(data_root_retention),
            layout="data-root",
        )
    if database_root_retention.is_dir():
        return InfluxTree(
            role=role,
            input_root=str(input_root),
            database_root=str(input_root),
            retention_root=str(database_root_retention),
            layout="database-root",
        )
    raise RuntimeError(
        f"Could not resolve {role} tree from {input_root}. "
        f"Expected either {input_root / database / retention} or {input_root / retention}."
    )


def enumerate_tsm_files(retention_root: Path) -> list[TsmFile]:
    if not retention_root.is_dir():
        raise RuntimeError(f"Retention directory not found: {retention_root}")

    files: list[TsmFile] = []
    for shard_dir in sorted(retention_root.iterdir(), key=lambda path: int(path.name) if path.name.isdigit() else path.name):
        if not shard_dir.is_dir() or not shard_dir.name.isdigit():
            continue
        shard_id = int(shard_dir.name)
        try:
            shard_files = sorted(shard_dir.glob("*.tsm"))
        except PermissionError as exc:
            raise RuntimeError(
                f"Permission denied while reading shard path {shard_dir}. "
                "Run this helper as a user that can read the InfluxDB data tree, typically: sudo -u influxdb ..."
            ) from exc
        for path in shard_files:
            generation, sequence = parse_tsm_name(path.name)
            files.append(
                TsmFile(
                    shard_id=shard_id,
                    generation=generation,
                    sequence=sequence,
                    rel_path=f"{shard_id}/{path.name}",
                    abs_path=str(path),
                    size_bytes=path.stat().st_size,
                )
            )
    return files


def ensure_pipeline_state(root: Path, args: argparse.Namespace, files: list[TsmFile], data_tree: InfluxTree) -> dict[str, Any]:
    current = {
        "pipeline_version": PIPELINE_VERSION,
        "measurement": MEASUREMENT,
        "database": args.database,
        "retention": args.retention,
        "data_dir": str(Path(args.data_dir).resolve()),
        "wal_dir": str(Path(args.wal_dir).resolve()),
        "data_layout": data_tree.layout,
        "database_root": data_tree.database_root,
        "retention_root": data_tree.retention_root,
        "start": args.start,
        "end": args.end,
        "inst_ids": sorted(set(args.inst_ids or [])),
    }
    path = pipeline_state_path(root)
    existing = safe_load_json(path)

    file_index = {
        file.file_id: {
            "rel_path": file.rel_path,
            "abs_path": file.abs_path,
            "shard_id": file.shard_id,
            "size_bytes": file.size_bytes,
            "generation": file.generation,
            "sequence": file.sequence,
        }
        for file in files
    }

    if existing is None:
        if any(root.iterdir()):
            raise RuntimeError(
                f"{root} already contains files from an older or incompatible run. "
                "Rerun with --overwrite to rebuild the new resumable pipeline state."
            )
        state = {
            "meta": current,
            "created_at": exporter.utc_now_iso(),
            "updated_at": exporter.utc_now_iso(),
            "files": {
                file_id: {
                    **meta,
                    "scan_status": "pending",
                    "has_measurement": None,
                    "inst_ids": [],
                    "export_status": "pending",
                    "export_stream_bytes": 0,
                    "export_matched_lines": 0,
                    "min_time_ns": None,
                    "max_time_ns": None,
                }
                for file_id, meta in file_index.items()
            },
            "merge": {},
            "build": {},
        }
        save_json(path, state)
        return state

    if existing.get("meta") != current:
        raise RuntimeError(
            "Existing pipeline state does not match this request. "
            "Rerun with --overwrite or use the same database/retention/start/end/instId set."
        )

    existing_files = existing.get("files", {})
    if set(existing_files) != set(file_index):
        raise RuntimeError(
            "Current TSM file inventory does not match the existing pipeline state. "
            "Rerun with --overwrite to rebuild the scan/export/merge pipeline."
        )

    for file_id, meta in file_index.items():
        state_entry = existing_files[file_id]
        for key, value in meta.items():
            if state_entry.get(key) != value:
                raise RuntimeError(
                    f"TSM metadata changed for {file_id}. "
                    "Rerun with --overwrite to rebuild the pipeline state."
                )

    existing["updated_at"] = exporter.utc_now_iso()
    save_json(path, existing)
    return existing


def save_pipeline_state(root: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = exporter.utc_now_iso()
    save_json(pipeline_state_path(root), state)


def scan_single_tsm(inspect_binary: str, source_file: TsmFile) -> ScanResult:
    started = time.monotonic()
    command = [
        inspect_binary,
        "dumptsm",
        "-index",
        "-filter-key",
        f"{MEASUREMENT},",
        source_file.abs_path,
    ]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(
            f"dumptsm failed for {source_file.rel_path} with exit code {result.returncode}: {stderr}"
        )
    text = result.stdout or ""
    inst_ids = sorted(set(lp_unescape(item) for item in INST_ID_RE.findall(text)))
    has_measurement = bool(inst_ids or MEASUREMENT in text)
    return ScanResult(
        file_id=source_file.file_id,
        rel_path=source_file.rel_path,
        abs_path=source_file.abs_path,
        shard_id=source_file.shard_id,
        size_bytes=source_file.size_bytes,
        generation=source_file.generation,
        sequence=source_file.sequence,
        has_measurement=has_measurement,
        inst_ids=inst_ids,
        elapsed_seconds=time.monotonic() - started,
    )


def build_index_summary(root: Path, state: dict[str, Any]) -> dict[str, Any]:
    files = state["files"]
    matched = [entry for entry in files.values() if entry.get("has_measurement")]
    summary = {
        "measurement": MEASUREMENT,
        "database": state["meta"]["database"],
        "retention": state["meta"]["retention"],
        "data_dir": state["meta"]["data_dir"],
        "data_layout": state["meta"].get("data_layout"),
        "database_root": state["meta"].get("database_root"),
        "retention_root": state["meta"].get("retention_root"),
        "created_at": exporter.utc_now_iso(),
        "scan_complete": all(entry.get("scan_status") == "complete" for entry in files.values()),
        "total_tsm_files": len(files),
        "total_tsm_bytes": sum(int(entry["size_bytes"]) for entry in files.values()),
        "matched_tsm_files": len(matched),
        "matched_tsm_bytes": sum(int(entry["size_bytes"]) for entry in matched),
        "inst_ids": sorted({inst_id for entry in matched for inst_id in entry.get("inst_ids", [])}),
        "files": [
            {
                "file_id": entry["file_id"] if "file_id" in entry else file_id,
                "rel_path": entry["rel_path"],
                "shard_id": entry["shard_id"],
                "size_bytes": entry["size_bytes"],
                "generation": entry["generation"],
                "sequence": entry["sequence"],
                "inst_ids": entry.get("inst_ids", []),
            }
            for file_id, entry in sorted(files.items())
            if entry.get("has_measurement")
        ],
    }
    save_json(tsm_index_path(root), summary)
    return summary


def run_scan_phase(root: Path, args: argparse.Namespace, state: dict[str, Any], files: list[TsmFile]) -> dict[str, Any]:
    pending = [file for file in files if state["files"][file.file_id]["scan_status"] != "complete"]
    if not pending:
        summary = build_index_summary(root, state)
        print(
            "[scan] "
            f"already-complete files={summary['matched_tsm_files']}/{summary['total_tsm_files']} "
            f"matched_size={human_bytes(summary['matched_tsm_bytes'])}",
            flush=True,
        )
        return summary

    worker_count = max(1, args.workers)
    futures: dict[Future[ScanResult], str] = {}
    submit_index = 0
    completed = len(files) - len(pending)
    progress = ScanPhaseProgress(
        total_files=len(files),
        total_bytes=sum(file.size_bytes for file in files),
        progress_interval=max(1, args.progress_interval),
        completed_files=completed,
        completed_bytes=sum(
            int(entry["size_bytes"])
            for entry in state["files"].values()
            if entry.get("scan_status") == "complete"
        ),
        matched_files=sum(1 for entry in state["files"].values() if entry.get("has_measurement")),
        matched_bytes=sum(
            int(entry["size_bytes"])
            for entry in state["files"].values()
            if entry.get("has_measurement")
        ),
    )

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        while submit_index < len(pending) or futures:
            while len(futures) < worker_count and submit_index < len(pending):
                source_file = pending[submit_index]
                state["files"][source_file.file_id]["scan_status"] = "running"
                save_pipeline_state(root, state)
                futures[executor.submit(scan_single_tsm, args.inspect_binary, source_file)] = source_file.file_id
                submit_index += 1

            done, _ = wait(list(futures), timeout=max(1, args.progress_interval), return_when=FIRST_COMPLETED)
            if not done:
                progress.maybe_print(running=len(futures))
                continue
            for future in done:
                file_id = futures.pop(future)
                result = future.result()
                entry = state["files"][file_id]
                entry["scan_status"] = "complete"
                entry["has_measurement"] = result.has_measurement
                entry["inst_ids"] = result.inst_ids
                save_pipeline_state(root, state)
                completed += 1
                progress.note_completed(result.size_bytes, result.has_measurement)
                print(
                    "[scan-file] "
                    f"done={completed}/{len(files)} "
                    f"file={result.rel_path} "
                    f"matched={'yes' if result.has_measurement else 'no'} "
                    f"inst_ids={','.join(result.inst_ids) or '-'} "
                    f"elapsed={human_duration(result.elapsed_seconds)}",
                    flush=True,
                )

    summary = build_index_summary(root, state)
    print(
        "[scan-done] "
        f"total={summary['total_tsm_files']} "
        f"matched={summary['matched_tsm_files']} "
        f"matched_size={human_bytes(summary['matched_tsm_bytes'])} "
        f"inst_ids={','.join(summary['inst_ids']) or '-'}",
        flush=True,
    )
    return summary


def determine_target_inst_ids(state: dict[str, Any], allowed_inst_ids: set[str]) -> list[str]:
    discovered = sorted(
        {
            inst_id
            for entry in state["files"].values()
            if entry.get("has_measurement")
            for inst_id in entry.get("inst_ids", [])
        }
    )
    if allowed_inst_ids:
        missing = sorted(allowed_inst_ids - set(discovered))
        if missing:
            raise RuntimeError(
                f"Requested instId values were not found in scanned TSM files: {', '.join(missing)}"
            )
        return sorted(allowed_inst_ids)
    return discovered


def export_single_tsm(
    args: argparse.Namespace,
    source_file: TsmFile,
    allowed_inst_ids: set[str],
    root: Path,
    data_tree: InfluxTree,
    wal_tree: InfluxTree,
) -> ExportResult:
    started = time.monotonic()
    raw_dir = raw_root(root) / source_file.file_id
    if raw_dir.exists():
        shutil.rmtree(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    command = [
        args.inspect_binary,
        "export",
        "-database",
        args.database,
        "-retention",
        args.retention,
        "-datadir",
        data_tree.input_root,
        "-waldir",
        wal_tree.input_root,
        "-tsmfile",
        source_file.abs_path,
        "-start",
        args.start,
        "-end",
        args.end,
        "-lponly",
        "-out",
        "-",
    ]

    field_handles: dict[tuple[str, str], tuple[Any, Any]] = {}
    field_line_counts: dict[str, dict[str, int]] = {}
    progress = ExportRuntimeProgress(
        file_id=source_file.file_id,
        rel_path=source_file.rel_path,
        progress_interval=args.progress_interval,
    )
    min_time_ns: int | None = None
    max_time_ns: int | None = None

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    if process.stdout is None:
        raise RuntimeError(f"influx_inspect export did not provide stdout for {source_file.rel_path}.")
    line_queue = BoundedTextQueue(args.export_queue_bytes)
    reader_error: dict[str, BaseException] = {}
    reader_thread = threading.Thread(
        target=pump_process_stdout,
        args=(process.stdout, line_queue, reader_error),
        daemon=True,
    )
    reader_thread.start()
    try:
        while True:
            queued = line_queue.get()
            if queued is None:
                break
            raw_line, raw_line_bytes = queued
            parsed = parse_okex_depth_lp_line(raw_line)
            if parsed is None:
                progress.note_line(raw_line, None, None, size_bytes=raw_line_bytes)
                continue

            inst_id, parsed_fields, timestamp_ns = parsed
            if allowed_inst_ids and inst_id not in allowed_inst_ids:
                progress.note_line(raw_line, None, None, size_bytes=raw_line_bytes)
                continue
            matched_fields = {
                field_name: value
                for field_name, value in parsed_fields.items()
                if field_name in OKEX_DEPTH_FIELDS
            }
            if not matched_fields:
                progress.note_line(raw_line, None, None, size_bytes=raw_line_bytes)
                continue

            progress.note_line(raw_line, inst_id, timestamp_ns, size_bytes=raw_line_bytes)

            if min_time_ns is None or timestamp_ns < min_time_ns:
                min_time_ns = timestamp_ns
            if max_time_ns is None or timestamp_ns > max_time_ns:
                max_time_ns = timestamp_ns

            field_counts = field_line_counts.setdefault(inst_id, {})
            for field_name, value in matched_fields.items():
                field_counts[field_name] = field_counts.get(field_name, 0) + 1

                spool_path = compressed_field_path(raw_dir / inst_id, field_name)
                handle_key = (inst_id, field_name)
                handle_entry = field_handles.get(handle_key)
                if handle_entry is None:
                    context = open_spool_text_writer(spool_path, compression_level=1)
                    handle = context.__enter__()
                    field_handles[handle_key] = (context, handle)
                else:
                    _, handle = handle_entry
                append_spool_line(handle, timestamp_ns, value)
        reader_thread.join()
        if "error" in reader_error:
            raise reader_error["error"]
    except BaseException:
        line_queue.close()
        if process.poll() is None:
            process.terminate()
        reader_thread.join(timeout=5)
        if process.poll() is None:
            process.kill()
        raise
    finally:
        line_queue.close()
        for context, handle in field_handles.values():
            try:
                handle.flush()
            finally:
                context.__exit__(None, None, None)

    return_code = process.wait()
    progress.maybe_print(force=True)
    if return_code != 0:
        raise RuntimeError(
            f"influx_inspect export failed for {source_file.rel_path} with exit code {return_code}."
        )

    matched_lines = sum(sum(fields.values()) for fields in field_line_counts.values())
    result = ExportResult(
        file_id=source_file.file_id,
        rel_path=source_file.rel_path,
        shard_id=source_file.shard_id,
        generation=source_file.generation,
        sequence=source_file.sequence,
        raw_dir=str(raw_dir),
        stream_bytes=progress.bytes_read,
        matched_lines=matched_lines,
        field_line_counts=field_line_counts,
        inst_ids=sorted(field_line_counts),
        min_time_ns=min_time_ns,
        max_time_ns=max_time_ns,
        elapsed_seconds=time.monotonic() - started,
    )
    save_json(raw_manifest_path(root, source_file.file_id), asdict(result))
    return result


def run_export_phase(
    root: Path,
    args: argparse.Namespace,
    state: dict[str, Any],
    source_files: list[TsmFile],
    target_inst_ids: set[str],
    data_tree: InfluxTree,
    wal_tree: InfluxTree,
) -> None:
    selected = [
        file
        for file in source_files
        if state["files"][file.file_id].get("has_measurement")
        and (not target_inst_ids or target_inst_ids.intersection(state["files"][file.file_id].get("inst_ids", [])))
    ]
    pending: list[TsmFile] = []
    for file in selected:
        entry = state["files"][file.file_id]
        if entry["export_status"] == "complete" and export_artifact_complete(root, file.file_id):
            continue
        pending.append(file)
    if not pending:
        print(
            "[export] "
            f"already-complete files={len(selected)} "
            f"target_inst_ids={','.join(sorted(target_inst_ids)) or '-'}",
            flush=True,
        )
        return

    worker_count = max(1, args.workers)
    futures: dict[Future[ExportResult], str] = {}
    submit_index = 0
    completed = len(selected) - len(pending)

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        while submit_index < len(pending) or futures:
            while len(futures) < worker_count and submit_index < len(pending):
                source_file = pending[submit_index]
                state["files"][source_file.file_id]["export_status"] = "running"
                save_pipeline_state(root, state)
                futures[
                    executor.submit(export_single_tsm, args, source_file, target_inst_ids, root, data_tree, wal_tree)
                ] = source_file.file_id
                submit_index += 1

            done, _ = wait(list(futures), return_when=FIRST_COMPLETED)
            for future in done:
                file_id = futures.pop(future)
                result = future.result()
                entry = state["files"][file_id]
                entry["export_status"] = "complete"
                entry["export_stream_bytes"] = result.stream_bytes
                entry["export_matched_lines"] = result.matched_lines
                entry["min_time_ns"] = result.min_time_ns
                entry["max_time_ns"] = result.max_time_ns
                save_pipeline_state(root, state)
                completed += 1
                print(
                    "[export-done] "
                    f"done={completed}/{len(selected)} "
                    f"file={result.rel_path} "
                    f"lines={result.matched_lines} "
                    f"stream={human_bytes(result.stream_bytes)} "
                    f"inst_ids={','.join(result.inst_ids) or '-'} "
                    f"elapsed={human_duration(result.elapsed_seconds)}",
                    flush=True,
                )


def gather_merge_sources(root: Path, state: dict[str, Any], target_inst_ids: set[str]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    sources: dict[str, dict[str, list[dict[str, Any]]]] = {
        inst_id: {field_name: [] for field_name in OKEX_DEPTH_FIELDS}
        for inst_id in sorted(target_inst_ids)
    }
    for file_id, entry in state["files"].items():
        if entry.get("export_status") != "complete":
            continue
        manifest = safe_load_json(raw_manifest_path(root, file_id))
        if manifest is None:
            raise RuntimeError(
                f"Export manifest missing for completed file {file_id}. "
                "Rerun the export phase to regenerate raw fragments."
            )
        raw_dir = Path(manifest["raw_dir"])
        if not raw_dir.exists():
            raise RuntimeError(
                f"Raw export directory missing for completed file {file_id}: {raw_dir}. "
                "Rerun the export phase to regenerate raw fragments."
            )
        for inst_id, field_counts in manifest.get("field_line_counts", {}).items():
            if target_inst_ids and inst_id not in target_inst_ids:
                continue
            for field_name, count in field_counts.items():
                if count <= 0:
                    continue
                path = resolve_existing_field_path(raw_dir / inst_id, field_name)
                if path is None:
                    raise RuntimeError(
                        f"Raw field fragment missing for completed file {file_id}: {raw_dir / inst_id}. "
                        "Rerun the export phase to regenerate raw fragments."
                    )
                sources[inst_id][field_name].append(
                    {
                        "file_id": file_id,
                        "path": str(path),
                        "shard_id": int(entry["shard_id"]),
                        "generation": int(entry["generation"]),
                        "sequence": int(entry["sequence"]),
                    }
                )
    for inst_id in sources:
        for field_name in sources[inst_id]:
            sources[inst_id][field_name].sort(
                key=lambda item: (item["shard_id"], item["generation"], item["sequence"], item["file_id"])
            )
    return sources


def merged_signature_for_inst(inst_sources: dict[str, list[dict[str, Any]]]) -> dict[str, list[str]]:
    return {
        field_name: [item["file_id"] for item in items]
        for field_name, items in inst_sources.items()
    }


def merge_single_field(
    root: Path,
    inst_id: str,
    field_name: str,
    sources: list[dict[str, Any]],
    progress_interval: int,
) -> MergeResult:
    started = time.monotonic()
    dest_path = merged_field_path(root, inst_id, field_name)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if dest_path.exists():
        dest_path.unlink()

    if not sources:
        dest_path.write_text("", encoding="utf-8")
        return MergeResult(
            inst_id=inst_id,
            field_name=field_name,
            dest_path=str(dest_path),
            row_count=0,
            source_count=0,
            source_files=[],
            elapsed_seconds=time.monotonic() - started,
        )

    iterators: list[Iterator[tuple[int, Any]]] = [collapsed_tsv_iter(Path(item["path"])) for item in sources]
    current_items: list[tuple[int, Any] | None] = [next(iterator, None) for iterator in iterators]
    heap: list[tuple[int, int, Any, int]] = []
    for rank, item in enumerate(current_items):
        if item is None:
            continue
        timestamp, value = item
        heapq.heappush(heap, (timestamp, rank, value, rank))

    row_count = 0
    progress = MergeRuntimeProgress(
        inst_id=inst_id,
        field_name=field_name,
        source_count=len(sources),
        progress_interval=max(1, progress_interval),
    )
    with open_spool_text_writer(dest_path, compression_level=3) as handle:
        while heap:
            timestamp, rank, value, source_index = heapq.heappop(heap)
            same_timestamp: list[tuple[int, int, Any, int]] = [(timestamp, rank, value, source_index)]
            while heap and heap[0][0] == timestamp:
                same_timestamp.append(heapq.heappop(heap))

            chosen = max(same_timestamp, key=lambda item: item[1])
            append_spool_line(handle, timestamp, chosen[2])
            row_count += 1
            progress.note_row(timestamp)

            for _, _, _, idx in same_timestamp:
                next_item = next(iterators[idx], None)
                if next_item is not None:
                    next_ts, next_value = next_item
                    heapq.heappush(heap, (next_ts, idx, next_value, idx))
    progress.maybe_print(force=True)

    return MergeResult(
        inst_id=inst_id,
        field_name=field_name,
        dest_path=str(dest_path),
        row_count=row_count,
        source_count=len(sources),
        source_files=[item["file_id"] for item in sources],
        elapsed_seconds=time.monotonic() - started,
    )


def run_merge_phase(root: Path, args: argparse.Namespace, state: dict[str, Any], target_inst_ids: list[str]) -> None:
    sources = gather_merge_sources(root, state, set(target_inst_ids))
    merge_state = state.setdefault("merge", {})

    tasks: list[tuple[str, str, list[dict[str, Any]]]] = []
    for inst_id in target_inst_ids:
        inst_state = merge_state.setdefault(inst_id, {})
        for field_name in OKEX_DEPTH_FIELDS:
            source_list = sources[inst_id][field_name]
            source_ids = [item["file_id"] for item in source_list]
            field_state = inst_state.setdefault(
                field_name,
                {"status": "pending", "row_count": 0, "source_count": 0, "source_files": [], "dest_path": ""},
            )
            dest_path = merged_field_path(root, inst_id, field_name)
            if (
                field_state.get("status") == "complete"
                and field_state.get("source_files") == source_ids
                and dest_path.exists()
            ):
                continue
            tasks.append((inst_id, field_name, source_list))

    if not tasks:
        print("[merge] already-complete", flush=True)
        return

    worker_count = max(1, args.workers)
    futures: dict[Future[MergeResult], tuple[str, str]] = {}
    submit_index = 0
    completed = len(target_inst_ids) * len(OKEX_DEPTH_FIELDS) - len(tasks)

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        while submit_index < len(tasks) or futures:
            while len(futures) < worker_count and submit_index < len(tasks):
                inst_id, field_name, source_list = tasks[submit_index]
                state["merge"][inst_id][field_name]["status"] = "running"
                save_pipeline_state(root, state)
                futures[executor.submit(
                    merge_single_field,
                    root,
                    inst_id,
                    field_name,
                    source_list,
                    args.progress_interval,
                )] = (
                    inst_id,
                    field_name,
                )
                submit_index += 1

            done, _ = wait(list(futures), return_when=FIRST_COMPLETED)
            for future in done:
                inst_id, field_name = futures.pop(future)
                result = future.result()
                state["merge"][inst_id][field_name] = {
                    "status": "complete",
                    "row_count": result.row_count,
                    "source_count": result.source_count,
                    "source_files": result.source_files,
                    "dest_path": result.dest_path,
                }
                save_pipeline_state(root, state)
                completed += 1
                print(
                    "[merge-field] "
                    f"done={completed}/{len(target_inst_ids) * len(OKEX_DEPTH_FIELDS)} "
                    f"instId={inst_id} "
                    f"field={field_name} "
                    f"rows={result.row_count} "
                    f"sources={result.source_count} "
                    f"elapsed={human_duration(result.elapsed_seconds)}",
                    flush=True,
                )


def build_single_dataset(root: Path, args: argparse.Namespace, inst_id: str) -> BuildResult:
    started = time.monotonic()
    stage_dataset = build_stage_dataset_path(root, inst_id)
    if stage_dataset.exists():
        shutil.rmtree(stage_dataset)
    stage_dataset.mkdir(parents=True, exist_ok=True)
    progress = BuildRuntimeProgress(inst_id=inst_id, progress_interval=max(1, args.progress_interval))

    schema = exporter.build_schema(MEASUREMENT, OKEX_DEPTH_FIELD_TYPES, OKEX_DEPTH_TAG_KEYS)
    builder = ParquetDatasetBuilder(
        schema=schema,
        dataset_path=stage_dataset,
        rows_per_file=args.rows_per_file,
        compression=args.compression,
        compression_level=args.compression_level,
        progress=progress,
    )

    existing = {}
    for field_name in OKEX_DEPTH_FIELDS:
        path = resolve_existing_field_path(merged_root(root) / inst_id, field_name)
        if path is not None and path.stat().st_size > 0:
            existing[field_name] = path
    pipeline_state = safe_load_json(pipeline_state_path(root)) or {}
    merge_state = pipeline_state.get("merge", {}).get(inst_id, {})
    merged_signature = {
        field_name: {
            "row_count": field_state.get("row_count", 0),
            "source_files": list(field_state.get("source_files", [])),
        }
        for field_name, field_state in merge_state.items()
        if field_state.get("status") == "complete"
    }

    if not existing:
        progress.maybe_print(force=True)
        return BuildResult(
            inst_id=inst_id,
            stage_dataset_path=str(stage_dataset),
            rows=0,
            part_count=0,
            bytes_written=0,
            min_time_ns=None,
            max_time_ns=None,
            merged_signature=merged_signature,
            elapsed_seconds=time.monotonic() - started,
        )

    iterators = {field_name: collapsed_tsv_iter(path) for field_name, path in existing.items()}
    current_items: dict[str, tuple[int, Any] | None] = {
        field_name: next(iterator, None) for field_name, iterator in iterators.items()
    }

    while any(item is not None for item in current_items.values()):
        next_timestamp = min(item[0] for item in current_items.values() if item is not None)
        row: dict[str, Any] = {"time": next_timestamp, "instId": inst_id}
        for field_name, current in list(current_items.items()):
            if current is None or current[0] != next_timestamp:
                continue
            row[field_name] = current[1]
            current_items[field_name] = next(iterators[field_name], None)
        builder.append_row(row)

    stats = builder.finalize()
    progress.maybe_print(force=True)
    return BuildResult(
        inst_id=inst_id,
        stage_dataset_path=str(stage_dataset),
        rows=stats.rows,
        part_count=stats.part_count,
        bytes_written=stats.bytes_written,
        min_time_ns=stats.min_time_ns,
        max_time_ns=stats.max_time_ns,
        merged_signature=merged_signature,
        elapsed_seconds=time.monotonic() - started,
    )


def commit_built_dataset(
    root: Path,
    args: argparse.Namespace,
    result: BuildResult,
    data_tree: InfluxTree,
    wal_tree: InfluxTree,
) -> None:
    stage_dataset = Path(result.stage_dataset_path)
    final_dataset = measurement_root(args.output_dir) / f"{result.inst_id}.parquet"
    if final_dataset.exists():
        shutil.rmtree(final_dataset)
    shutil.move(str(stage_dataset), str(final_dataset))

    checkpoint = initialize_final_checkpoint(
        dataset_path=final_dataset,
        inst_id=result.inst_id,
        database=args.database,
        start=args.start,
        end=args.end,
        rows_per_file=args.rows_per_file,
        compression=args.compression,
        compression_level=args.compression_level,
    )
    checkpoint.part_index = result.part_count
    checkpoint.rows_written = result.rows
    checkpoint.chunks_flushed = result.part_count
    checkpoint.bytes_written = result.bytes_written
    checkpoint.min_time_ns = result.min_time_ns
    checkpoint.max_time_ns = result.max_time_ns
    checkpoint.completed = True
    checkpoint.completed_series = [checkpoint.series_key] if checkpoint.series_key else []
    checkpoint.active_series = None
    checkpoint.active_series_last_time_ns = None
    exporter.save_checkpoint(exporter.checkpoint_path(final_dataset), checkpoint)

    schema = exporter.build_schema(MEASUREMENT, OKEX_DEPTH_FIELD_TYPES, OKEX_DEPTH_TAG_KEYS)
    write_dataset_metadata(final_dataset, checkpoint, result.inst_id, schema, source_command_template(args, data_tree, wal_tree))


def run_build_phase(
    root: Path,
    args: argparse.Namespace,
    state: dict[str, Any],
    target_inst_ids: list[str],
    data_tree: InfluxTree,
    wal_tree: InfluxTree,
) -> None:
    build_state = state.setdefault("build", {})
    merge_state = state.get("merge", {})

    total = len(target_inst_ids)
    completed = 0

    for inst_id in target_inst_ids:
        merged_signature = {
            field_name: {
                "row_count": field_state.get("row_count", 0),
                "source_files": list(field_state.get("source_files", [])),
            }
            for field_name, field_state in merge_state.get(inst_id, {}).items()
            if field_state.get("status") == "complete"
        }
        final_dataset = measurement_root(args.output_dir) / f"{inst_id}.parquet"
        entry = build_state.setdefault(
            inst_id,
            {"status": "pending", "rows": 0, "part_count": 0, "bytes_written": 0, "merged_signature": {}},
        )
        if (
            entry.get("status") == "complete"
            and entry.get("merged_signature") == merged_signature
            and entry.get("rows_per_file") == args.rows_per_file
            and entry.get("compression") == args.compression
            and entry.get("compression_level") == args.compression_level
            and final_dataset.exists()
            and checkpoint_exists(final_dataset)
        ):
            completed += 1
            continue

        entry["status"] = "running"
        save_pipeline_state(root, state)
        result = build_single_dataset(root, args, inst_id)
        commit_built_dataset(root, args, result, data_tree, wal_tree)
        entry.update(
            {
                "status": "complete",
                "rows": result.rows,
                "part_count": result.part_count,
                "bytes_written": result.bytes_written,
                "merged_signature": result.merged_signature,
                "rows_per_file": args.rows_per_file,
                "compression": args.compression,
                "compression_level": args.compression_level,
            }
        )
        save_pipeline_state(root, state)
        completed += 1
        print(
            "[build-dataset] "
            f"done={completed}/{total} "
            f"instId={inst_id} "
            f"rows={result.rows} "
            f"parts={result.part_count} "
            f"size={human_bytes(result.bytes_written)} "
            f"elapsed={human_duration(result.elapsed_seconds)}",
            flush=True,
        )


def main() -> int:
    args = parse_args()
    args.start = exporter.parse_rfc3339(args.start)
    args.end = exporter.parse_rfc3339(args.end)
    if parse_utc(args.end) <= parse_utc(args.start):
        raise SystemExit("--end must be later than --start.")

    root = measurement_root(args.output_dir)
    ensure_clean_root(root, args.overwrite)

    try:
        data_tree = resolve_influx_tree(Path(args.data_dir), args.database, args.retention, role="data")
        source_files = enumerate_tsm_files(Path(data_tree.retention_root))
        if not source_files:
            print("[done] no .tsm files found under the requested database/retention.", flush=True)
            return 0

        state = ensure_pipeline_state(root, args, source_files, data_tree)
        raw_root(root).mkdir(parents=True, exist_ok=True)
        merged_root(root).mkdir(parents=True, exist_ok=True)
        build_root(root).mkdir(parents=True, exist_ok=True)

        print(
            "[plan] "
            f"tsm_files={len(source_files)} "
            f"tsm_size={human_bytes(sum(file.size_bytes for file in source_files))} "
            f"source_layout={data_tree.layout} "
            f"retention_root={data_tree.retention_root} "
            f"workers={max(1, args.workers)}",
            flush=True,
        )

        scan_summary = run_scan_phase(root, args, state, source_files)
        if args.stop_after == "scan":
            print("[stop] after scan", flush=True)
            return 0

        wal_tree = resolve_influx_tree(Path(args.wal_dir), args.database, args.retention, role="wal")

        target_inst_ids = determine_target_inst_ids(state, set(args.inst_ids or []))
        if not target_inst_ids:
            raise RuntimeError("No okex_depth instId values were discovered in scanned TSM files.")
        print(
            "[target] "
            f"inst_ids={','.join(target_inst_ids)} "
            f"matched_files={scan_summary['matched_tsm_files']} "
            f"wal_layout={wal_tree.layout}",
            flush=True,
        )

        run_export_phase(root, args, state, source_files, set(target_inst_ids), data_tree, wal_tree)
        if args.stop_after == "export":
            print("[stop] after export", flush=True)
            return 0

        run_merge_phase(root, args, state, target_inst_ids)
        if args.stop_after == "merge":
            print("[stop] after merge", flush=True)
            return 0

        run_build_phase(root, args, state, target_inst_ids, data_tree, wal_tree)
        print(
            "[done] "
            f"measurement={MEASUREMENT} "
            f"inst_ids={len(target_inst_ids)} "
            f"output={root}",
            flush=True,
        )
        return 0
    except KeyboardInterrupt:
        print("[interrupted] already completed tasks are preserved and will resume on the next run.", flush=True)
        return 130
    except RuntimeError as exc:
        print(f"[error] {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
