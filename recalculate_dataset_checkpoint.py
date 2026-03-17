#!/usr/bin/env python3
"""Rebuild exporter checkpoint metadata from surviving Parquet parts."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import extract_optiondata_to_parquet as exporter


PART_PATTERN = re.compile(r"part-(\d{6})\.parquet$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Recalculate a dataset checkpoint after Parquet part loss. "
            "The script scans surviving part files, derives the last exported series/time, "
            "and rewrites _checkpoint.json plus summary/description metadata."
        )
    )
    parser.add_argument("dataset_path", help="Path to the dataset directory ending in .parquet")
    parser.add_argument("--host", default="127.0.0.1", help="InfluxDB host. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=8086, help="InfluxDB HTTP port. Default: 8086")
    parser.add_argument(
        "--database",
        help="InfluxDB database name. Defaults to the value recorded in the checkpoint or Parquet metadata.",
    )
    parser.add_argument("--measurement", help="Measurement name override when checkpoint is missing.")
    parser.add_argument("--start", help="Original inclusive export start timestamp override.")
    parser.add_argument("--end", help="Original exclusive export end timestamp override.")
    parser.add_argument("--limit", type=int, help="Original export row limit override.")
    parser.add_argument("--series-key", help="Original exact series key override.")
    parser.add_argument("--username", help="Optional InfluxDB username.")
    parser.add_argument("--password", help="Optional InfluxDB password.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the recalculated checkpoint payload without writing files.",
    )
    return parser.parse_args()


def load_existing_checkpoint(dataset_path: Path) -> exporter.Checkpoint:
    path = exporter.checkpoint_path(dataset_path)
    if not path.exists():
        raise RuntimeError(f"Checkpoint file not found: {path}")
    return exporter.load_checkpoint(path)


def dataset_name_without_suffix(dataset_path: Path) -> str:
    return dataset_path.name[:-8] if dataset_path.name.endswith(".parquet") else dataset_path.name


def part_files(dataset_path: Path) -> list[tuple[int, Path]]:
    indexed_parts: list[tuple[int, Path]] = []
    for path in sorted(dataset_path.glob("part-*.parquet")):
        match = PART_PATTERN.fullmatch(path.name)
        if match is None:
            continue
        indexed_parts.append((int(match.group(1)), path))

    if not indexed_parts:
        return []

    indices = [index for index, _ in indexed_parts]
    expected = list(range(len(indices)))
    if indices != expected:
        raise RuntimeError(
            "Dataset part files are not a contiguous prefix from part-000000.parquet. "
            f"Found indices: {indices[:10]}{'...' if len(indices) > 10 else ''}"
        )
    return indexed_parts


def infer_compression_codec(path: Path) -> str:
    parquet = pq.ParquetFile(path)
    metadata = parquet.metadata
    if metadata.num_row_groups == 0:
        return "unknown"
    row_group = metadata.row_group(0)
    if row_group.num_columns == 0:
        return "unknown"
    codec = row_group.column(0).compression
    return str(codec).lower() if codec is not None else "unknown"


def metadata_value(schema: pa.Schema, key: bytes) -> str | None:
    metadata = schema.metadata or {}
    value = metadata.get(key)
    if value is None:
        return None
    return value.decode("utf-8")


def bootstrap_checkpoint_from_parts(dataset_path: Path, args: argparse.Namespace) -> exporter.Checkpoint:
    parts = part_files(dataset_path)
    if not parts:
        raise RuntimeError(
            f"{dataset_path} has no checkpoint and no part files. Nothing to rebuild."
        )

    first_part = parts[0][1]
    schema = pq.ParquetFile(first_part).schema_arrow
    measurement = args.measurement or metadata_value(schema, b"measurement") or dataset_name_without_suffix(dataset_path)
    database = args.database or metadata_value(schema, b"database") or "OptionData"
    compression = infer_compression_codec(first_part)

    return exporter.Checkpoint(
        measurement=measurement,
        database=database,
        dataset_path=str(dataset_path),
        start=args.start,
        end=args.end,
        limit=args.limit,
        chunk_size=20_000,
        rows_per_file=1_000_000,
        compression=compression,
        compression_level=9,
        series_key=args.series_key,
        rows_per_file_history=[1_000_000],
    )


def first_row_values(path: Path, columns: list[str]) -> dict[str, str]:
    if not columns:
        return {}

    batch = next(pq.ParquetFile(path).iter_batches(batch_size=1, columns=columns), None)
    if batch is None or batch.num_rows == 0:
        return {}

    table = pa.Table.from_batches([batch])
    row: dict[str, str] = {}
    for column in columns:
        value = table[column][0].as_py()
        if value is not None:
            row[column] = str(value)
    return row


def part_time_range_ns(path: Path) -> tuple[int, int]:
    table = pq.read_table(path, columns=["time"])
    if table.num_rows == 0:
        raise RuntimeError(f"Parquet part has no rows: {path}")
    time_column = table.column("time").combine_chunks()
    time_ns = pc.cast(time_column, pa.int64())
    min_ns = pc.min(time_ns).as_py()
    max_ns = pc.max(time_ns).as_py()
    if min_ns is None or max_ns is None:
        raise RuntimeError(f"Failed to derive time range from part: {path}")
    return int(min_ns), int(max_ns)


def build_series_lookup(client: exporter.InfluxClient, measurement: str) -> tuple[list[str], dict[tuple[tuple[str, str], ...], str]]:
    tag_keys = client.tag_keys(measurement)
    series_lookup: dict[tuple[tuple[str, str], ...], str] = {}
    for series_key in client.show_series_keys(measurement):
        _, tags = exporter.parse_series_key(series_key)
        key = tuple(sorted(tags.items()))
        if key in series_lookup:
            raise RuntimeError(f"Duplicate series key mapping detected for measurement {measurement}: {series_key}")
        series_lookup[key] = series_key
    return tag_keys, series_lookup


def lookup_series_key(
    checkpoint: exporter.Checkpoint,
    client: exporter.InfluxClient,
    part_path: Path,
    cached: tuple[list[str], dict[tuple[tuple[str, str], ...], str]] | None,
) -> tuple[str | None, tuple[list[str], dict[tuple[tuple[str, str], ...], str]] | None]:
    if checkpoint.series_key is not None:
        return checkpoint.series_key, cached

    if cached is None:
        cached = build_series_lookup(client, checkpoint.measurement)
    tag_keys, series_lookup = cached
    if not series_lookup:
        return None, cached

    part_tags = first_row_values(part_path, tag_keys)
    key = tuple(sorted(part_tags.items()))
    series_key = series_lookup.get(key)
    if series_key is None:
        raise RuntimeError(
            f"Could not map Parquet tags back to an Influx series for {part_path.name}. "
            f"Observed tags: {part_tags}"
        )
    return series_key, cached


def rebuild_checkpoint(
    dataset_path: Path,
    existing: exporter.Checkpoint,
    client: exporter.InfluxClient,
) -> tuple[exporter.Checkpoint, int]:
    rebuilt = exporter.Checkpoint(**asdict(existing))
    parts = part_files(dataset_path)
    rebuilt.dataset_path = str(dataset_path)
    rebuilt.part_index = len(parts)
    rebuilt.rows_written = 0
    rebuilt.chunks_flushed = len(parts)
    rebuilt.bytes_written = 0
    rebuilt.min_time_ns = None
    rebuilt.max_time_ns = None
    rebuilt.completed_series = []
    rebuilt.active_series = None
    rebuilt.active_series_last_time_ns = None
    rebuilt.completed = False

    if not parts:
        return rebuilt, 0

    cached_lookup: tuple[list[str], dict[tuple[tuple[str, str], ...], str]] | None = None
    series_sequence: list[str] = []
    active_series = None
    active_series_last_time_ns = None

    for _, part_path in parts:
        metadata = pq.ParquetFile(part_path).metadata
        rows = metadata.num_rows
        min_time_ns, max_time_ns = part_time_range_ns(part_path)
        series_key, cached_lookup = lookup_series_key(existing, client, part_path, cached_lookup)

        rebuilt.rows_written += rows
        rebuilt.bytes_written += part_path.stat().st_size
        rebuilt.min_time_ns = min(min_time_ns, rebuilt.min_time_ns) if rebuilt.min_time_ns is not None else min_time_ns
        rebuilt.max_time_ns = max(max_time_ns, rebuilt.max_time_ns) if rebuilt.max_time_ns is not None else max_time_ns

        if series_key is not None and (not series_sequence or series_sequence[-1] != series_key):
            series_sequence.append(series_key)
        active_series = series_key
        active_series_last_time_ns = max_time_ns

    if active_series is not None:
        rebuilt.completed_series = series_sequence[:-1]
        rebuilt.active_series = active_series
        rebuilt.active_series_last_time_ns = active_series_last_time_ns

    return rebuilt, len(series_sequence)


def refresh_description_files(
    dataset_path: Path,
    checkpoint: exporter.Checkpoint,
    client: exporter.InfluxClient,
    derived_series_count: int,
) -> None:
    field_types = client.field_types(checkpoint.measurement)
    tag_keys = client.tag_keys(checkpoint.measurement)
    schema = exporter.build_schema(checkpoint.measurement, field_types, tag_keys)
    query_template = exporter.build_query_template_for_series(
        checkpoint.measurement,
        checkpoint.series_key,
        checkpoint.start,
        checkpoint.end,
        checkpoint.limit,
    )
    if checkpoint.series_key is not None:
        series_count = 1
    else:
        series_count = derived_series_count or len(client.show_series_keys(checkpoint.measurement))

    exporter.write_description_files(
        dataset_path=dataset_path,
        measurement=checkpoint.measurement,
        query_template=query_template,
        schema=schema,
        field_types=field_types,
        tag_keys=tag_keys,
        series_count=series_count,
        checkpoint=checkpoint,
    )


def main() -> int:
    args = parse_args()
    dataset_path = Path(args.dataset_path).resolve()
    try:
        existing = load_existing_checkpoint(dataset_path)
    except RuntimeError:
        existing = bootstrap_checkpoint_from_parts(dataset_path, args)
    database = args.database or existing.database

    client = exporter.InfluxClient(
        host=args.host,
        port=args.port,
        database=database,
        username=args.username,
        password=args.password,
    )

    rebuilt, derived_series_count = rebuild_checkpoint(dataset_path, existing, client)

    if args.dry_run:
        print(json.dumps(asdict(rebuilt), indent=2, sort_keys=True))
        return 0

    exporter.save_checkpoint(exporter.checkpoint_path(dataset_path), rebuilt)
    refresh_description_files(dataset_path, rebuilt, client, derived_series_count)

    print(
        "[recalculated] "
        f"dataset={dataset_path} rows={rebuilt.rows_written} "
        f"parts={rebuilt.part_index} size={exporter.human_bytes(rebuilt.bytes_written)} "
        f"active_series={rebuilt.active_series or '-'} "
        f"resume_after={exporter.ns_to_iso8601(rebuilt.active_series_last_time_ns) or '-'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
