#!/usr/bin/env python3
"""Export one approved OptionData measurement from InfluxDB to a Parquet dataset."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import requests

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ModuleNotFoundError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(
        "Missing dependency: pyarrow. Install it in the active environment before running this script."
    ) from exc


EXCLUDED_MEASUREMENTS = {
    "chainlink_eth_price",
    "deribit_perpetual",
    "settle_prices",
    "target_position",
    "hedge_target_position",
    "candlestick_chart",
    "candlestick_chart_option_price",
    "candlestick_chart_option_price_test",
    "candlstick",
    "deribit_delta_recorder",
    "deribit_option",
    "dex_option_price",
    "dex_option_price_model",
    "implied_voltality",
    "option",
    "option_market_metrics_online",
    "option_market_metrics_symbol_profit_online",
    "option_market_metrics_test",
    "option_market_metrics_total_profit_online",
    "option_price",
    "option_profit_daily_merge_test",
    "option_settle_prices",
    "options",
    "predict_product_price",
    "predict_product_price_delta_test",
    "predict_product_price_test",
    "test",
    "test_settle_price_table",
}

INFLUX_TO_ARROW = {
    "float": pa.float64(),
    "integer": pa.int64(),
    "string": pa.string(),
    "boolean": pa.bool_(),
}


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export one non-option market-data measurement from the InfluxDB OptionData "
            "database into a resumable compressed Parquet dataset."
        )
    )
    parser.add_argument("measurement", help="Measurement to export.")
    parser.add_argument("--host", default="127.0.0.1", help="InfluxDB host.")
    parser.add_argument("--port", type=int, default=8086, help="InfluxDB HTTP port.")
    parser.add_argument("--database", default="OptionData", help="InfluxDB database name.")
    parser.add_argument(
        "--output-dir",
        default="parquet_output",
        help="Directory where the measurement dataset will be written.",
    )
    parser.add_argument(
        "--dataset-name",
        help=(
            "Optional dataset directory name without the .parquet suffix. "
            "Defaults to the measurement name."
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
        help="Optional overall row limit for smoke tests or controlled exports.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=20000,
        help="InfluxDB chunk size for streamed queries. Default: 20000.",
    )
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=1_000_000,
        help="Target rows per Parquet part file before flushing. Default: 1000000.",
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
        "--series-key",
        help=(
            "Optional exact Influx series key to export, for example "
            "'okex_depth,instId=BTC-USDT-SWAP'. When set, only that single series is exported."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete any existing dataset for this measurement before exporting.",
    )
    parser.add_argument(
        "--allow-excluded",
        action="store_true",
        help="Allow exporting measurements that are excluded by project policy.",
    )
    return parser.parse_args()


def parse_rfc3339(value: str) -> str:
    normalized = value.replace("Z", "+00:00")
    try:
        timestamp = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid RFC3339 timestamp: {value}") from exc

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    else:
        timestamp = timestamp.astimezone(UTC)
    return timestamp.isoformat().replace("+00:00", "Z")


def quote_ident(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def quote_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def ns_to_iso8601(value: int | None) -> str | None:
    if value is None:
        return None
    seconds, nanos = divmod(value, 1_000_000_000)
    base = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    if nanos:
        return f"{base}.{nanos:09d}Z"
    return f"{base}Z"


def human_bytes(value: int) -> str:
    size = float(value)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{value} B"


def split_escaped(text: str, separator: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    escaped = False
    for char in text:
        if escaped:
            current.append(char)
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == separator:
            parts.append("".join(current))
            current = []
            continue
        current.append(char)
    if escaped:
        current.append("\\")
    parts.append("".join(current))
    return parts


def split_first_escaped(text: str, separator: str) -> tuple[str, str]:
    current: list[str] = []
    escaped = False
    for index, char in enumerate(text):
        if escaped:
            current.append(char)
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == separator:
            return "".join(current), text[index + 1 :]
        current.append(char)
    return "".join(current), ""


def influx_unescape(text: str) -> str:
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


def parse_series_key(series_key: str) -> tuple[str, dict[str, str]]:
    parts = split_escaped(series_key, ",")
    measurement = influx_unescape(parts[0])
    tags: dict[str, str] = {}
    for raw_part in parts[1:]:
        key, value = split_first_escaped(raw_part, "=")
        tags[influx_unescape(key)] = influx_unescape(value)
    return measurement, tags


def iter_series(payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for result in payload.get("results", []):
        error = result.get("error")
        if error:
            raise RuntimeError(error)
        for series in result.get("series", []):
            yield series


@dataclass
class InfluxClient:
    host: str
    port: int
    database: str
    username: str | None = None
    password: str | None = None
    connect_timeout: int = 10
    read_timeout: int = 60

    def __post_init__(self) -> None:
        self.base_url = f"http://{self.host}:{self.port}/query"
        self.session = requests.Session()
        self.session.trust_env = False

    def _auth(self) -> tuple[str, str] | None:
        if self.username and self.password:
            return self.username, self.password
        return None

    def query_json(self, query: str) -> dict[str, Any]:
        response = self.session.get(
            self.base_url,
            params={"db": self.database, "q": query, "epoch": "ns"},
            auth=self._auth(),
            timeout=(self.connect_timeout, self.read_timeout),
        )
        response.raise_for_status()
        payload = response.json()
        list(iter_series(payload))
        return payload

    def query_chunks(self, query: str, chunk_size: int) -> Iterable[dict[str, Any]]:
        response = self.session.get(
            self.base_url,
            params={
                "db": self.database,
                "q": query,
                "epoch": "ns",
                "chunked": "true",
                "chunk_size": str(chunk_size),
            },
            auth=self._auth(),
            stream=True,
            timeout=(self.connect_timeout, None),
        )
        response.raise_for_status()

        with response:
            for raw_line in response.iter_lines():
                if not raw_line:
                    continue
                payload = json.loads(raw_line)
                list(iter_series(payload))
                yield payload

    def show_series_exists(self, measurement: str) -> bool:
        payload = self.query_json(f"SHOW SERIES FROM {quote_ident(measurement)} LIMIT 1")
        return any(True for _ in iter_series(payload))

    def show_series_keys(self, measurement: str) -> list[str]:
        payload = self.query_json(f"SHOW SERIES FROM {quote_ident(measurement)}")
        keys: list[str] = []
        for series in iter_series(payload):
            if series.get("columns") != ["key"]:
                continue
            for (key,) in series.get("values", []):
                keys.append(key)
        return sorted(keys)

    def field_types(self, measurement: str) -> dict[str, str]:
        payload = self.query_json(f"SHOW FIELD KEYS FROM {quote_ident(measurement)}")
        field_types: dict[str, str] = {}
        for series in iter_series(payload):
            if series.get("columns") != ["fieldKey", "fieldType"]:
                continue
            for field_name, field_type in series.get("values", []):
                field_types[field_name] = field_type
        return field_types

    def tag_keys(self, measurement: str) -> list[str]:
        payload = self.query_json(f"SHOW TAG KEYS FROM {quote_ident(measurement)}")
        tag_keys: list[str] = []
        for series in iter_series(payload):
            if series.get("columns") != ["tagKey"]:
                continue
            for (tag_key,) in series.get("values", []):
                tag_keys.append(tag_key)
        return sorted(tag_keys)


@dataclass
class Checkpoint:
    measurement: str
    database: str
    dataset_path: str
    start: str | None
    end: str | None
    limit: int | None
    chunk_size: int
    rows_per_file: int
    compression: str
    compression_level: int
    series_key: str | None = None
    rows_per_file_history: list[int] = field(default_factory=list)
    part_index: int = 0
    rows_written: int = 0
    chunks_flushed: int = 0
    bytes_written: int = 0
    min_time_ns: int | None = None
    max_time_ns: int | None = None
    completed_series: list[str] = field(default_factory=list)
    active_series: str | None = None
    active_series_last_time_ns: int | None = None
    completed: bool = False
    started_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass
class ProgressState:
    measurement: str
    dataset_path: Path
    rows_written: int
    chunks_flushed: int
    parts_written: int
    bytes_written: int
    min_time_ns: int | None
    max_time_ns: int | None
    started_at: float = field(default_factory=time.monotonic)
    buffer_rows: int = 0
    current_series: str | None = None
    current_series_min_time_ns: int | None = None
    current_series_max_time_ns: int | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)

    def note_series_start(self, series_key: str) -> None:
        with self.lock:
            self.current_series = series_key
            self.current_series_min_time_ns = None
            self.current_series_max_time_ns = None

    def note_buffer(self, rows: int, min_time_ns: int | None, max_time_ns: int | None, series_key: str) -> None:
        with self.lock:
            self.buffer_rows += rows
            if self.current_series != series_key:
                self.current_series = series_key
                self.current_series_min_time_ns = min_time_ns
                self.current_series_max_time_ns = max_time_ns
            else:
                if min_time_ns is not None and (
                    self.current_series_min_time_ns is None or min_time_ns < self.current_series_min_time_ns
                ):
                    self.current_series_min_time_ns = min_time_ns
                if max_time_ns is not None and (
                    self.current_series_max_time_ns is None or max_time_ns > self.current_series_max_time_ns
                ):
                    self.current_series_max_time_ns = max_time_ns
            if min_time_ns is not None and (
                self.min_time_ns is None or min_time_ns < self.min_time_ns
            ):
                self.min_time_ns = min_time_ns
            if max_time_ns is not None and (
                self.max_time_ns is None or max_time_ns > self.max_time_ns
            ):
                self.max_time_ns = max_time_ns

    def note_flush(self, rows: int, chunks: int, bytes_written: int, current_series: str | None) -> None:
        with self.lock:
            self.rows_written += rows
            self.buffer_rows = max(0, self.buffer_rows - rows)
            self.chunks_flushed += chunks
            self.parts_written += 1
            self.bytes_written += bytes_written
            self.current_series = current_series

    def note_series_complete(self) -> None:
        with self.lock:
            self.current_series = None
            self.current_series_min_time_ns = None
            self.current_series_max_time_ns = None

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "measurement": self.measurement,
                "rows_written": self.rows_written,
                "buffer_rows": self.buffer_rows,
                "chunks_flushed": self.chunks_flushed,
                "parts_written": self.parts_written,
                "bytes_written": self.bytes_written,
                "min_time_ns": self.min_time_ns,
                "max_time_ns": self.max_time_ns,
                "current_series": self.current_series,
                "current_series_min_time_ns": self.current_series_min_time_ns,
                "current_series_max_time_ns": self.current_series_max_time_ns,
                "elapsed_seconds": time.monotonic() - self.started_at,
            }


def progress_worker(state: ProgressState, stop_event: threading.Event) -> None:
    while not stop_event.wait(5):
        snapshot = state.snapshot()
        print(
            "[progress] "
            f"measurement={snapshot['measurement']} "
            f"series={snapshot['current_series'] or '-'} "
            f"rows_written={snapshot['rows_written']} "
            f"buffer_rows={snapshot['buffer_rows']} "
            f"parts={snapshot['parts_written']} "
            f"range={ns_to_iso8601(snapshot['current_series_min_time_ns']) or '-'}.."
            f"{ns_to_iso8601(snapshot['current_series_max_time_ns']) or '-'} "
            f"dataset_size={human_bytes(snapshot['bytes_written'])} "
            f"elapsed={int(snapshot['elapsed_seconds'])}s",
            flush=True,
        )


def checkpoint_path(dataset_path: Path) -> Path:
    return dataset_path / "_checkpoint.json"


def description_path(dataset_path: Path) -> Path:
    return dataset_path / "_description.md"


def summary_path(dataset_path: Path) -> Path:
    return dataset_path / "_summary.json"


def save_checkpoint(path: Path, checkpoint: Checkpoint) -> None:
    checkpoint.updated_at = utc_now_iso()
    temp_path = path.with_suffix(".tmp")
    temp_path.write_text(json.dumps(asdict(checkpoint), indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def load_checkpoint(path: Path) -> Checkpoint:
    return Checkpoint(**json.loads(path.read_text(encoding="utf-8")))


def ensure_compatible_checkpoint(checkpoint: Checkpoint, args: argparse.Namespace, dataset_path: Path) -> None:
    current = {
        "measurement": args.measurement,
        "database": args.database,
        "dataset_path": str(dataset_path),
        "start": args.start,
        "end": args.end,
        "limit": args.limit,
        "series_key": args.series_key,
    }
    existing = {
        "measurement": checkpoint.measurement,
        "database": checkpoint.database,
        "dataset_path": checkpoint.dataset_path,
        "start": checkpoint.start,
        "end": checkpoint.end,
        "limit": checkpoint.limit,
        "series_key": checkpoint.series_key,
    }
    if current != existing:
        raise RuntimeError(
            "Existing checkpoint does not match this export request. "
            "Use the same measurement/start/end/limit values or rerun with --overwrite."
        )


def sync_resume_tuning(checkpoint: Checkpoint, args: argparse.Namespace) -> None:
    if checkpoint.rows_per_file_history:
        history = list(checkpoint.rows_per_file_history)
    else:
        history = [checkpoint.rows_per_file]

    if checkpoint.rows_per_file != args.rows_per_file:
        checkpoint.rows_per_file = args.rows_per_file
        if history[-1] != args.rows_per_file:
            history.append(args.rows_per_file)

    checkpoint.rows_per_file_history = history


def initialize_checkpoint(args: argparse.Namespace, dataset_path: Path) -> Checkpoint:
    return Checkpoint(
        measurement=args.measurement,
        database=args.database,
        dataset_path=str(dataset_path),
        start=args.start,
        end=args.end,
        limit=args.limit,
        chunk_size=args.chunk_size,
        rows_per_file=args.rows_per_file,
        compression=args.compression,
        compression_level=args.compression_level,
        series_key=args.series_key,
        rows_per_file_history=[args.rows_per_file],
    )


def prepare_dataset_path(args: argparse.Namespace) -> Path:
    dataset_name = args.dataset_name or args.measurement
    dataset_path = Path(args.output_dir).resolve() / f"{dataset_name}.parquet"
    if args.overwrite and dataset_path.exists():
        shutil.rmtree(dataset_path)
    dataset_path.mkdir(parents=True, exist_ok=True)
    for temp_file in dataset_path.glob("*.tmp"):
        temp_file.unlink()
    return dataset_path


def load_or_create_checkpoint(args: argparse.Namespace, dataset_path: Path) -> Checkpoint:
    path = checkpoint_path(dataset_path)
    if path.exists():
        checkpoint = load_checkpoint(path)
        ensure_compatible_checkpoint(checkpoint, args, dataset_path)
        if not checkpoint.completed:
            sync_resume_tuning(checkpoint, args)
            save_checkpoint(path, checkpoint)
        return checkpoint

    existing_parts = list(dataset_path.glob("part-*.parquet"))
    if existing_parts:
        raise RuntimeError(
            f"{dataset_path} already contains Parquet part files but no checkpoint. "
            "Inspect the dataset or rerun with --overwrite."
        )

    checkpoint = initialize_checkpoint(args, dataset_path)
    save_checkpoint(path, checkpoint)
    return checkpoint


def build_base_time_filters(start: str | None, end: str | None, resume_after_ns: int | None) -> list[str]:
    filters: list[str] = []
    if start:
        filters.append(f"time >= {quote_string(parse_rfc3339(start))}")
    if end:
        filters.append(f"time < {quote_string(parse_rfc3339(end))}")
    if resume_after_ns is not None:
        filters.append(f"time > {quote_string(ns_to_iso8601(resume_after_ns) or '')}")
    return filters


def build_series_query(
    measurement: str,
    series_key: str,
    start: str | None,
    end: str | None,
    resume_after_ns: int | None,
    limit: int | None,
) -> str:
    _, tags = parse_series_key(series_key)
    filters = [f"{quote_ident(key)} = {quote_string(value)}" for key, value in sorted(tags.items())]
    filters.extend(build_base_time_filters(start, end, resume_after_ns))
    where_clause = " WHERE " + " AND ".join(filters) if filters else ""
    query = f"SELECT * FROM {quote_ident(measurement)}{where_clause} ORDER BY time ASC"
    if limit is not None:
        query += f" LIMIT {limit}"
    return query


def build_query_template(measurement: str, start: str | None, end: str | None, limit: int | None) -> str:
    filters = ["<series-tag-filters>"]
    filters.extend(build_base_time_filters(start, end, None))
    where_clause = " WHERE " + " AND ".join(filters)
    query = f"SELECT * FROM {quote_ident(measurement)}{where_clause} ORDER BY time ASC"
    if limit is not None:
        query += " LIMIT <remaining-limit>"
    return query


def build_query_template_for_series(
    measurement: str,
    series_key: str | None,
    start: str | None,
    end: str | None,
    limit: int | None,
) -> str:
    if series_key is None:
        return build_query_template(measurement, start, end, limit)

    _, tags = parse_series_key(series_key)
    filters = [f"{quote_ident(key)} = {quote_string(value)}" for key, value in sorted(tags.items())]
    filters.extend(build_base_time_filters(start, end, None))
    where_clause = " WHERE " + " AND ".join(filters) if filters else ""
    query = f"SELECT * FROM {quote_ident(measurement)}{where_clause} ORDER BY time ASC"
    if limit is not None:
        query += f" LIMIT {limit}"
    return query


def arrow_type_for_column(name: str, field_types: dict[str, str], tag_keys: set[str]) -> pa.DataType:
    if name == "time":
        return pa.timestamp("ns", tz="UTC")
    if name in tag_keys:
        return pa.string()
    return INFLUX_TO_ARROW.get(field_types.get(name), pa.string())


def build_schema(measurement: str, field_types: dict[str, str], tag_keys: list[str]) -> pa.Schema:
    ordered_columns = ["time"] + sorted(tag_keys) + sorted(field_types)
    schema = pa.schema(
        [
            pa.field(column, arrow_type_for_column(column, field_types, set(tag_keys)))
            for column in ordered_columns
        ]
    )
    metadata = {
        b"measurement": measurement.encode(),
        b"database": b"OptionData",
    }
    return schema.with_metadata(metadata)


def column_values(values: list[list[Any]], index: int) -> list[Any]:
    return [row[index] if index < len(row) else None for row in values]


def convert_to_array(values: list[Any], data_type: pa.DataType) -> pa.Array:
    if pa.types.is_timestamp(data_type):
        return pa.array(values, type=data_type)
    if pa.types.is_string(data_type):
        return pa.array([None if value is None else str(value) for value in values], type=data_type)
    return pa.array(values, type=data_type)


def table_from_series(series: dict[str, Any], schema: pa.Schema) -> pa.Table:
    source_columns = series.get("columns", [])
    values = series.get("values", [])
    if not values:
        return pa.table({})

    column_lookup = {name: index for index, name in enumerate(source_columns)}
    arrays = []
    for field in schema:
        source_index = column_lookup.get(field.name)
        raw_values = [None] * len(values) if source_index is None else column_values(values, source_index)
        arrays.append(convert_to_array(raw_values, field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def extract_series_time_range(series: dict[str, Any]) -> tuple[int | None, int | None]:
    columns = series.get("columns", [])
    values = series.get("values", [])
    if not values:
        return None, None
    try:
        time_index = columns.index("time")
    except ValueError:
        return None, None

    time_values = [row[time_index] for row in values if row[time_index] is not None]
    if not time_values:
        return None, None
    return min(time_values), max(time_values)


def schema_description_rows(schema: pa.Schema, field_types: dict[str, str], tag_keys: set[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for field in schema:
        if field.name == "time":
            kind = "time"
            influx_type = "timestamp(ns)"
        elif field.name in tag_keys:
            kind = "tag"
            influx_type = "string"
        else:
            kind = "field"
            influx_type = field_types.get(field.name, "unknown")
        rows.append(
            {
                "name": field.name,
                "kind": kind,
                "influx_type": influx_type,
                "parquet_type": str(field.type),
            }
        )
    return rows


def write_part_file(
    dataset_path: Path,
    part_index: int,
    schema: pa.Schema,
    tables: list[pa.Table],
    compression: str,
    compression_level: int,
) -> tuple[Path, int]:
    final_path = dataset_path / f"part-{part_index:06d}.parquet"
    temp_path = dataset_path / f".part-{part_index:06d}.tmp"
    with pq.ParquetWriter(
        temp_path,
        schema,
        compression=compression,
        compression_level=compression_level,
        use_dictionary=True,
        write_statistics=True,
    ) as writer:
        for table in tables:
            writer.write_table(table)
    temp_path.replace(final_path)
    return final_path, final_path.stat().st_size


def write_description_files(
    dataset_path: Path,
    measurement: str,
    query_template: str,
    schema: pa.Schema,
    field_types: dict[str, str],
    tag_keys: list[str],
    series_count: int,
    checkpoint: Checkpoint,
) -> None:
    description_rows = schema_description_rows(schema, field_types, set(tag_keys))
    rows_per_file_history = checkpoint.rows_per_file_history or [checkpoint.rows_per_file]
    summary = {
        "measurement": measurement,
        "dataset_path": str(dataset_path),
        "record_count": checkpoint.rows_written,
        "date_range": {
            "min_time_ns": checkpoint.min_time_ns,
            "max_time_ns": checkpoint.max_time_ns,
            "min_time": ns_to_iso8601(checkpoint.min_time_ns),
            "max_time": ns_to_iso8601(checkpoint.max_time_ns),
        },
        "compression": {
            "codec": checkpoint.compression,
            "level": checkpoint.compression_level,
        },
        "part_count": checkpoint.part_index,
        "dataset_size_bytes": checkpoint.bytes_written,
        "dataset_size_human": human_bytes(checkpoint.bytes_written),
        "series_count": series_count,
        "rows_per_file_target": checkpoint.rows_per_file,
        "rows_per_file_history": rows_per_file_history,
        "chunk_size": checkpoint.chunk_size,
        "query_template": query_template,
        "table_format": description_rows,
    }
    summary_path(dataset_path).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        f"# Measurement Export Description: {measurement}",
        "",
        "## Overview",
        f"- Dataset path: `{dataset_path}`",
        f"- Dataset size: `{human_bytes(checkpoint.bytes_written)}`",
        f"- Part files: `{checkpoint.part_index}`",
        f"- Series count: `{series_count}`",
        f"- Record count: `{checkpoint.rows_written}`",
        f"- Date range: `{ns_to_iso8601(checkpoint.min_time_ns) or 'n/a'}` to `{ns_to_iso8601(checkpoint.max_time_ns) or 'n/a'}`",
        f"- Compression: `{checkpoint.compression}` level `{checkpoint.compression_level}`",
        f"- Target rows per part: `{checkpoint.rows_per_file}`",
    ]
    if len(rows_per_file_history) > 1:
        lines.append(
            f"- Rows per part history: `{ ' -> '.join(str(value) for value in rows_per_file_history) }`"
        )
    lines.extend(
        [
            "",
            "## Query Template",
            "```sql",
            query_template,
            "```",
            "",
            "## Table Format",
            "| Column | Kind | Influx Type | Parquet Type |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in description_rows:
        lines.append(
            f"| {row['name']} | {row['kind']} | {row['influx_type']} | {row['parquet_type']} |"
        )
    description_path(dataset_path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def export_measurement(
    client: InfluxClient,
    measurement: str,
    dataset_path: Path,
    checkpoint: Checkpoint,
    series_key_filter: str | None,
    start: str | None,
    end: str | None,
    limit: int | None,
    chunk_size: int,
    rows_per_file: int,
    compression: str,
    compression_level: int,
) -> dict[str, Any]:
    field_types = client.field_types(measurement)
    tag_keys = client.tag_keys(measurement)
    schema = build_schema(measurement, field_types, tag_keys)
    series_keys = client.show_series_keys(measurement)
    if series_key_filter is not None:
        if series_key_filter not in series_keys:
            raise RuntimeError(
                f"Requested series key '{series_key_filter}' was not found in measurement '{measurement}'."
            )
        series_keys = [series_key_filter]

    if not series_keys:
        checkpoint.completed = True
        save_checkpoint(checkpoint_path(dataset_path), checkpoint)
        return {"measurement": measurement, "status": "empty", "rows": 0}

    if checkpoint.completed:
        return {
            "measurement": measurement,
            "status": "already-complete",
            "rows": checkpoint.rows_written,
            "path": str(dataset_path),
            "file_size_bytes": checkpoint.bytes_written,
            "min_time": ns_to_iso8601(checkpoint.min_time_ns),
            "max_time": ns_to_iso8601(checkpoint.max_time_ns),
        }

    if checkpoint.active_series and checkpoint.active_series not in series_keys:
        raise RuntimeError(
            f"Checkpoint references missing series '{checkpoint.active_series}'. "
            "Inspect the dataset or rerun with --overwrite."
        )

    progress = ProgressState(
        measurement=measurement,
        dataset_path=dataset_path,
        rows_written=checkpoint.rows_written,
        chunks_flushed=checkpoint.chunks_flushed,
        parts_written=checkpoint.part_index,
        bytes_written=checkpoint.bytes_written,
        min_time_ns=checkpoint.min_time_ns,
        max_time_ns=checkpoint.max_time_ns,
    )
    stop_event = threading.Event()
    progress_thread = threading.Thread(target=progress_worker, args=(progress, stop_event), daemon=True)
    progress_thread.start()

    completed_series = list(checkpoint.completed_series)
    completed_series_set = set(completed_series)
    buffer_tables: list[pa.Table] = []
    buffer_rows = 0
    buffer_chunks = 0
    buffer_min_time_ns: int | None = None
    buffer_max_time_ns: int | None = None
    current_series_key: str | None = None
    status = "exported"

    def flush_buffer() -> None:
        nonlocal buffer_tables, buffer_rows, buffer_chunks, buffer_min_time_ns, buffer_max_time_ns
        if not buffer_tables or current_series_key is None:
            return

        _, bytes_written = write_part_file(
            dataset_path=dataset_path,
            part_index=checkpoint.part_index,
            schema=schema,
            tables=buffer_tables,
            compression=compression,
            compression_level=compression_level,
        )
        checkpoint.part_index += 1
        checkpoint.rows_written += buffer_rows
        checkpoint.chunks_flushed += buffer_chunks
        checkpoint.bytes_written += bytes_written
        checkpoint.active_series = current_series_key
        checkpoint.active_series_last_time_ns = buffer_max_time_ns
        if buffer_min_time_ns is not None and (
            checkpoint.min_time_ns is None or buffer_min_time_ns < checkpoint.min_time_ns
        ):
            checkpoint.min_time_ns = buffer_min_time_ns
        if buffer_max_time_ns is not None and (
            checkpoint.max_time_ns is None or buffer_max_time_ns > checkpoint.max_time_ns
        ):
            checkpoint.max_time_ns = buffer_max_time_ns
        save_checkpoint(checkpoint_path(dataset_path), checkpoint)
        progress.note_flush(buffer_rows, buffer_chunks, bytes_written, current_series_key)
        buffer_tables = []
        buffer_rows = 0
        buffer_chunks = 0
        buffer_min_time_ns = None
        buffer_max_time_ns = None

    try:
        for series_key in series_keys:
            if series_key in completed_series_set:
                continue

            remaining_limit = None if limit is None else max(limit - checkpoint.rows_written - buffer_rows, 0)
            if remaining_limit == 0:
                break

            resume_after_ns = checkpoint.active_series_last_time_ns if checkpoint.active_series == series_key else None
            checkpoint.active_series = series_key
            checkpoint.active_series_last_time_ns = resume_after_ns
            save_checkpoint(checkpoint_path(dataset_path), checkpoint)
            current_series_key = series_key
            progress.note_series_start(series_key)
            query = build_series_query(
                measurement=measurement,
                series_key=series_key,
                start=start,
                end=end,
                resume_after_ns=resume_after_ns,
                limit=remaining_limit,
            )

            for payload in client.query_chunks(query, chunk_size=chunk_size):
                for series in iter_series(payload):
                    table = table_from_series(series, schema)
                    if table.num_rows == 0:
                        continue
                    min_time_ns, max_time_ns = extract_series_time_range(series)
                    buffer_tables.append(table)
                    buffer_rows += table.num_rows
                    buffer_chunks += 1
                    if min_time_ns is not None and (
                        buffer_min_time_ns is None or min_time_ns < buffer_min_time_ns
                    ):
                        buffer_min_time_ns = min_time_ns
                    if max_time_ns is not None and (
                        buffer_max_time_ns is None or max_time_ns > buffer_max_time_ns
                    ):
                        buffer_max_time_ns = max_time_ns
                    progress.note_buffer(table.num_rows, min_time_ns, max_time_ns, series_key)
                    if buffer_rows >= rows_per_file:
                        flush_buffer()

            flush_buffer()
            checkpoint.active_series = None
            checkpoint.active_series_last_time_ns = None
            completed_series.append(series_key)
            completed_series_set.add(series_key)
            checkpoint.completed_series = completed_series
            save_checkpoint(checkpoint_path(dataset_path), checkpoint)
            progress.note_series_complete()

    except KeyboardInterrupt:
        status = "interrupted"
        flush_buffer()
        print(
            "[interrupt] checkpoint saved. Rerun the same command to resume automatically.",
            flush=True,
        )
    except Exception:
        flush_buffer()
        raise
    finally:
        stop_event.set()
        progress_thread.join(timeout=1)

    if status == "interrupted":
        return {
            "measurement": measurement,
            "status": "interrupted",
            "rows": checkpoint.rows_written,
            "path": str(dataset_path),
            "file_size_bytes": checkpoint.bytes_written,
            "min_time": ns_to_iso8601(checkpoint.min_time_ns),
            "max_time": ns_to_iso8601(checkpoint.max_time_ns),
        }

    checkpoint.completed = True
    checkpoint.active_series = None
    checkpoint.active_series_last_time_ns = None
    save_checkpoint(checkpoint_path(dataset_path), checkpoint)
    query_template = build_query_template_for_series(measurement, series_key_filter, start, end, limit)
    write_description_files(
        dataset_path=dataset_path,
        measurement=measurement,
        query_template=query_template,
        schema=schema,
        field_types=field_types,
        tag_keys=tag_keys,
        series_count=len(series_keys),
        checkpoint=checkpoint,
    )
    return {
        "measurement": measurement,
        "status": "exported",
        "rows": checkpoint.rows_written,
        "path": str(dataset_path),
        "file_size_bytes": checkpoint.bytes_written,
        "min_time": ns_to_iso8601(checkpoint.min_time_ns),
        "max_time": ns_to_iso8601(checkpoint.max_time_ns),
        "parts": checkpoint.part_index,
    }


def main() -> int:
    args = parse_args()

    if args.measurement in EXCLUDED_MEASUREMENTS and not args.allow_excluded:
        print(
            f"Measurement '{args.measurement}' is excluded by project policy. "
            "Use --allow-excluded only if you intentionally want to override that policy.",
            file=sys.stderr,
        )
        return 2

    dataset_path = prepare_dataset_path(args)
    checkpoint = load_or_create_checkpoint(args, dataset_path)

    client = InfluxClient(
        host=args.host,
        port=args.port,
        database=args.database,
        username=args.username,
        password=args.password,
    )

    if not client.show_series_exists(args.measurement):
        print(f"[done] measurement={args.measurement} status=empty rows=0 path={dataset_path}", flush=True)
        return 0

    if checkpoint.completed:
        print(
            f"[resume] measurement={args.measurement} "
            f"series={args.series_key or '-'} checkpoint=completed",
            flush=True,
        )
    elif checkpoint.rows_written > 0 or checkpoint.part_index > 0 or checkpoint.active_series:
        print(
            "[resume] "
            f"measurement={args.measurement} rows={checkpoint.rows_written} "
            f"parts={checkpoint.part_index} "
            f"series={args.series_key or '-'} "
            f"active_series={checkpoint.active_series or '-'}",
            flush=True,
        )
    else:
        print(
            f"[start] measurement={args.measurement} series={args.series_key or '-'}",
            flush=True,
        )

    result = export_measurement(
        client=client,
        measurement=args.measurement,
        dataset_path=dataset_path,
        checkpoint=checkpoint,
        series_key_filter=args.series_key,
        start=args.start,
        end=args.end,
        limit=args.limit,
        chunk_size=args.chunk_size,
        rows_per_file=args.rows_per_file,
        compression=args.compression,
        compression_level=args.compression_level,
    )

    print(
        "[done] "
        f"measurement={result['measurement']} "
        f"status={result['status']} "
        f"rows={result.get('rows', 0)} "
        f"path={result.get('path', '-')}",
        flush=True,
    )

    if result["status"] in {"exported", "already-complete"}:
        print(
            "[summary] "
            f"date_range={result.get('min_time')}..{result.get('max_time')} "
            f"dataset_size={human_bytes(result.get('file_size_bytes', 0))} "
            f"parts={result.get('parts', checkpoint.part_index)}",
            flush=True,
        )
        return 0

    if result["status"] == "interrupted":
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
