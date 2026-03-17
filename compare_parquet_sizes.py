#!/usr/bin/env python3
"""Compare on-disk and metadata-reported Parquet sizes for exported datasets."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

try:
    import pyarrow.parquet as pq
except ModuleNotFoundError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(
        "Missing dependency: pyarrow. Install it in the active environment before running this script."
    ) from exc


DEFAULT_ROOTS = [
    Path("/mnt/backup_hdd/exported"),
    Path("/mnt/backup_hdd/exported_parallel"),
]


@dataclass
class DatasetStats:
    root: str
    measurement: str
    dataset: str
    dataset_path: str
    status: str
    file_count: int
    row_group_count: int
    row_count: int
    actual_bytes: int
    compressed_bytes: int
    uncompressed_bytes: int

    @property
    def saved_bytes(self) -> int:
        return self.uncompressed_bytes - self.actual_bytes

    @property
    def actual_ratio(self) -> float | None:
        if self.actual_bytes <= 0:
            return None
        return self.uncompressed_bytes / self.actual_bytes

    @property
    def codec_ratio(self) -> float | None:
        if self.compressed_bytes <= 0:
            return None
        return self.uncompressed_bytes / self.compressed_bytes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan exported Parquet datasets and compare actual on-disk size with "
            "Parquet metadata compressed/uncompressed sizes."
        )
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help=(
            "Dataset or root directories to scan. Defaults to "
            "/mnt/backup_hdd/exported and /mnt/backup_hdd/exported_parallel."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of text tables.",
    )
    parser.add_argument(
        "--sort",
        choices=("measurement", "dataset", "actual", "uncompressed", "saved", "ratio"),
        default="measurement",
        help="Sort dataset rows by this key. Default: measurement.",
    )
    return parser.parse_args()


def human_bytes(value: int) -> str:
    size = float(value)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{value} B"


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return None


def dataset_part_files(dataset_dir: Path) -> list[Path]:
    return sorted(
        child for child in dataset_dir.iterdir() if child.is_file() and child.suffix == ".parquet"
    )


def is_dataset_dir(path: Path) -> bool:
    return path.is_dir() and path.suffix == ".parquet" and bool(dataset_part_files(path))


def discover_dataset_dirs(path: Path) -> list[Path]:
    if path.is_file() and path.suffix == ".parquet":
        return []
    if is_dataset_dir(path):
        return [path]
    if not path.is_dir():
        return []
    return sorted(candidate for candidate in path.rglob("*.parquet") if is_dataset_dir(candidate))


def dataset_status(dataset_dir: Path) -> str:
    checkpoint = read_json(dataset_dir / "_checkpoint.json")
    if checkpoint is not None:
        if bool(checkpoint.get("completed", False)):
            return "completed"
        return "active"
    if (dataset_dir / "_summary.json").exists():
        return "completed"
    return "unknown"


def measurement_from_metadata(dataset_dir: Path) -> str | None:
    part_files = dataset_part_files(dataset_dir)
    if not part_files:
        return None
    metadata = pq.ParquetFile(part_files[0]).schema_arrow.metadata or {}
    raw_value = metadata.get(b"measurement")
    if raw_value is None:
        return None
    try:
        return raw_value.decode("utf-8")
    except UnicodeDecodeError:
        return None


def derive_measurement(root: Path, dataset_dir: Path) -> tuple[str, str]:
    try:
        relative = dataset_dir.relative_to(root)
    except ValueError:
        relative = Path(dataset_dir.name)

    parts = relative.parts
    if not parts:
        return measurement_from_metadata(dataset_dir) or dataset_dir.stem, dataset_dir.name
    if len(parts) == 1:
        measurement = measurement_from_metadata(dataset_dir) or dataset_dir.stem
    elif parts[0].endswith(".parquet"):
        measurement = measurement_from_metadata(dataset_dir) or Path(parts[0]).stem
    else:
        measurement = parts[0]
    return measurement, str(relative)


def analyze_dataset(root: Path, dataset_dir: Path) -> DatasetStats:
    actual_bytes = 0
    compressed_bytes = 0
    uncompressed_bytes = 0
    row_count = 0
    row_group_count = 0

    part_files = dataset_part_files(dataset_dir)
    for part_file in part_files:
        actual_bytes += part_file.stat().st_size
        metadata = pq.ParquetFile(part_file).metadata
        row_count += metadata.num_rows
        row_group_count += metadata.num_row_groups
        for row_group_index in range(metadata.num_row_groups):
            row_group = metadata.row_group(row_group_index)
            for column_index in range(row_group.num_columns):
                column = row_group.column(column_index)
                compressed_bytes += column.total_compressed_size
                uncompressed_bytes += column.total_uncompressed_size

    measurement, dataset_name = derive_measurement(root, dataset_dir)
    return DatasetStats(
        root=str(root),
        measurement=measurement,
        dataset=dataset_name,
        dataset_path=str(dataset_dir),
        status=dataset_status(dataset_dir),
        file_count=len(part_files),
        row_group_count=row_group_count,
        row_count=row_count,
        actual_bytes=actual_bytes,
        compressed_bytes=compressed_bytes,
        uncompressed_bytes=uncompressed_bytes,
    )


def aggregate_measurements(datasets: list[DatasetStats]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for dataset in datasets:
        group = grouped.setdefault(
            dataset.measurement,
            {
                "measurement": dataset.measurement,
                "dataset_count": 0,
                "file_count": 0,
                "row_group_count": 0,
                "row_count": 0,
                "actual_bytes": 0,
                "compressed_bytes": 0,
                "uncompressed_bytes": 0,
                "completed_datasets": 0,
                "active_datasets": 0,
            },
        )
        group["dataset_count"] += 1
        group["file_count"] += dataset.file_count
        group["row_group_count"] += dataset.row_group_count
        group["row_count"] += dataset.row_count
        group["actual_bytes"] += dataset.actual_bytes
        group["compressed_bytes"] += dataset.compressed_bytes
        group["uncompressed_bytes"] += dataset.uncompressed_bytes
        if dataset.status == "completed":
            group["completed_datasets"] += 1
        elif dataset.status == "active":
            group["active_datasets"] += 1

    rows = []
    for group in grouped.values():
        actual = group["actual_bytes"]
        compressed = group["compressed_bytes"]
        uncompressed = group["uncompressed_bytes"]
        group["saved_bytes"] = uncompressed - actual
        group["actual_ratio"] = None if actual <= 0 else (uncompressed / actual)
        group["codec_ratio"] = None if compressed <= 0 else (uncompressed / compressed)
        rows.append(group)
    return sorted(rows, key=lambda row: row["measurement"])


def sort_datasets(rows: list[DatasetStats], sort_key: str) -> list[DatasetStats]:
    if sort_key == "measurement":
        return sorted(rows, key=lambda row: (row.measurement, row.dataset))
    if sort_key == "dataset":
        return sorted(rows, key=lambda row: row.dataset)
    if sort_key == "actual":
        return sorted(rows, key=lambda row: row.actual_bytes, reverse=True)
    if sort_key == "uncompressed":
        return sorted(rows, key=lambda row: row.uncompressed_bytes, reverse=True)
    if sort_key == "saved":
        return sorted(rows, key=lambda row: row.saved_bytes, reverse=True)
    if sort_key == "ratio":
        return sorted(rows, key=lambda row: row.actual_ratio or 0.0, reverse=True)
    return rows


def format_ratio(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}x"


def print_measurement_table(rows: list[dict[str, Any]]) -> None:
    print("Measurement totals")
    print(
        f"{'measurement':<22} {'datasets':>8} {'rows':>14} {'actual':>12} "
        f"{'uncompressed':>14} {'saved':>12} {'ratio':>8}"
    )
    for row in rows:
        print(
            f"{row['measurement']:<22} "
            f"{row['dataset_count']:>8} "
            f"{row['row_count']:>14,} "
            f"{human_bytes(row['actual_bytes']):>12} "
            f"{human_bytes(row['uncompressed_bytes']):>14} "
            f"{human_bytes(row['saved_bytes']):>12} "
            f"{format_ratio(row['actual_ratio']):>8}"
        )


def print_dataset_table(rows: list[DatasetStats]) -> None:
    print("\nDataset details")
    print(
        f"{'dataset':<44} {'status':<10} {'rows':>14} {'files':>7} "
        f"{'actual':>12} {'uncompressed':>14} {'saved':>12} {'ratio':>8}"
    )
    for row in rows:
        print(
            f"{row.dataset:<44.44} "
            f"{row.status:<10} "
            f"{row.row_count:>14,} "
            f"{row.file_count:>7} "
            f"{human_bytes(row.actual_bytes):>12} "
            f"{human_bytes(row.uncompressed_bytes):>14} "
            f"{human_bytes(row.saved_bytes):>12} "
            f"{format_ratio(row.actual_ratio):>8}"
        )


def main() -> int:
    args = parse_args()
    input_paths = [Path(path).resolve() for path in args.paths] if args.paths else DEFAULT_ROOTS

    datasets: list[DatasetStats] = []
    missing_paths: list[str] = []
    for input_path in input_paths:
        if not input_path.exists():
            missing_paths.append(str(input_path))
            continue
        for dataset_dir in discover_dataset_dirs(input_path):
            datasets.append(analyze_dataset(input_path, dataset_dir))

    if missing_paths:
        for missing_path in missing_paths:
            print(f"[warn] path not found: {missing_path}")

    if not datasets:
        print("No Parquet datasets found.", flush=True)
        return 1

    measurement_rows = aggregate_measurements(datasets)
    dataset_rows = sort_datasets(datasets, args.sort)

    if args.json:
        payload = {
            "roots": [str(path) for path in input_paths],
            "measurements": measurement_rows,
            "datasets": [asdict(row) | {"saved_bytes": row.saved_bytes, "actual_ratio": row.actual_ratio, "codec_ratio": row.codec_ratio} for row in dataset_rows],
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    print_measurement_table(measurement_rows)
    print_dataset_table(dataset_rows)
    print(
        "\nNotes\n"
        "- actual: real on-disk .parquet bytes\n"
        "- uncompressed: Parquet metadata total before codec compression\n"
        "- saved: uncompressed - actual\n"
        "- ratio: uncompressed / actual"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
