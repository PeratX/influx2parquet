#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import struct
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

try:
    import zstandard as zstd
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "missing dependency: zstandard\n"
        "run this script with /home/niko/influx2parquet/.venv/bin/python"
    ) from exc


DEFAULT_OLD_ROOT = Path("/mnt/backup_hdd/exported_parallel/okex_depth/_raw.bak")
DEFAULT_NEW_ROOT = Path("/mnt/backup_hdd/exported_parallel/okex_depth/_raw_go")
SPOOL_MAGIC = b"OKDSP01\n"
SPOOL_KIND_STRING = 1
SPOOL_KIND_INT64 = 2
FIELDS = ("action", "asks", "bids", "checksum", "ts")


@dataclass(frozen=True)
class DiffKey:
    source_id: str
    inst_id: str
    field: str


@dataclass
class RecordDiff:
    key: DiffKey
    old_count: int
    new_count: int
    old_min_time: Optional[int]
    old_max_time: Optional[int]
    new_min_time: Optional[int]
    new_max_time: Optional[int]
    first_mismatch: Optional[str] = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare old Python okex_depth raw TSV intermediates against new Go binary spool intermediates."
    )
    parser.add_argument("--old-root", type=Path, default=DEFAULT_OLD_ROOT, help="old raw root, default: %(default)s")
    parser.add_argument("--new-root", type=Path, default=DEFAULT_NEW_ROOT, help="new raw root, default: %(default)s")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=[],
        help="limit to one or more source ids such as shard-3511__000002005-000000006",
    )
    parser.add_argument(
        "--instid",
        action="append",
        dest="inst_ids",
        default=[],
        help="limit to one or more instIds",
    )
    parser.add_argument(
        "--field",
        action="append",
        dest="fields",
        default=[],
        help="limit to one or more fields",
    )
    parser.add_argument(
        "--verify-records",
        action="store_true",
        help="stream old/new files and compare every record for the selected sources/instIds/fields",
    )
    parser.add_argument(
        "--verify-mismatch-only",
        action="store_true",
        help="when used with --verify-records, only stream files that already mismatch at manifest level",
    )
    parser.add_argument(
        "--max-diffs",
        type=int,
        default=20,
        help="max mismatch lines to print in record verification mode",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def discover_manifests(root: Path) -> Dict[str, dict]:
    manifests: Dict[str, dict] = {}
    if not root.exists():
        return manifests
    for path in sorted(root.glob("*/_manifest.json")):
        manifests[path.parent.name] = load_json(path)
    return manifests


def manifest_field_counts(manifest: dict) -> Dict[str, Dict[str, int]]:
    raw = manifest.get("field_record_counts")
    if raw is None:
        raw = manifest.get("field_line_counts")
    return raw or {}


def manifest_total_records(manifest: dict) -> int:
    if "records" in manifest:
        return int(manifest["records"])
    if "matched_lines" in manifest:
        return int(manifest["matched_lines"])
    return 0


def manifest_min_time(manifest: dict) -> Optional[int]:
    value = manifest.get("min_time_ns")
    return int(value) if value is not None else None


def manifest_max_time(manifest: dict) -> Optional[int]:
    value = manifest.get("max_time_ns")
    return int(value) if value is not None else None


def iter_selected_keys(
    source_id: str,
    old_manifest: dict,
    new_manifest: dict,
    selected_inst_ids: set[str],
    selected_fields: set[str],
) -> Iterator[DiffKey]:
    inst_map: Dict[str, Dict[str, int]] = {}
    for mapping in (manifest_field_counts(old_manifest), manifest_field_counts(new_manifest)):
        for inst_id, fields in mapping.items():
            inst_map.setdefault(inst_id, {})
            for field in fields:
                inst_map[inst_id][field] = 1

    for inst_id in sorted(inst_map):
        if selected_inst_ids and inst_id not in selected_inst_ids:
            continue
        for field in sorted(inst_map[inst_id]):
            if selected_fields and field not in selected_fields:
                continue
            yield DiffKey(source_id=source_id, inst_id=inst_id, field=field)


def count_from_manifest(manifest: dict, inst_id: str, field: str) -> int:
    return int(manifest_field_counts(manifest).get(inst_id, {}).get(field, 0))


def manifest_path_for(key: DiffKey, root: Path, ext: str) -> Path:
    return root / key.source_id / key.inst_id / f"{key.field}.{ext}"


def compare_manifest_level(
    old_root: Path,
    new_root: Path,
    old_manifests: Dict[str, dict],
    new_manifests: Dict[str, dict],
    selected_sources: set[str],
    selected_inst_ids: set[str],
    selected_fields: set[str],
) -> Tuple[List[str], List[RecordDiff], List[str], List[str]]:
    old_sources = set(old_manifests)
    new_sources = set(new_manifests)
    if selected_sources:
        old_sources &= selected_sources
        new_sources &= selected_sources

    common_sources = sorted(old_sources & new_sources)
    old_only = sorted(old_sources - new_sources)
    new_only = sorted(new_sources - old_sources)

    notes: List[str] = []
    diffs: List[RecordDiff] = []
    for source_id in common_sources:
        old_manifest = old_manifests[source_id]
        new_manifest = new_manifests[source_id]

        old_total = manifest_total_records(old_manifest)
        new_total = manifest_total_records(new_manifest)
        old_min = manifest_min_time(old_manifest)
        old_max = manifest_max_time(old_manifest)
        new_min = manifest_min_time(new_manifest)
        new_max = manifest_max_time(new_manifest)

        if old_total != new_total or old_min != new_min or old_max != new_max:
            notes.append(
                f"[source] {source_id} total old={old_total} new={new_total} "
                f"time old={fmt_time_range(old_min, old_max)} new={fmt_time_range(new_min, new_max)}"
            )

        for key in iter_selected_keys(source_id, old_manifest, new_manifest, selected_inst_ids, selected_fields):
            old_count = count_from_manifest(old_manifest, key.inst_id, key.field)
            new_count = count_from_manifest(new_manifest, key.inst_id, key.field)
            old_path = manifest_path_for(key, old_root, "tsv.zst")
            new_path = manifest_path_for(key, new_root, "bin.zst")
            old_exists = old_path.exists()
            new_exists = new_path.exists()

            mismatch_reason = None
            if old_count != new_count:
                mismatch_reason = f"count old={old_count} new={new_count}"
            elif old_exists != new_exists:
                mismatch_reason = f"path old_exists={old_exists} new_exists={new_exists}"

            if mismatch_reason:
                diffs.append(
                    RecordDiff(
                        key=key,
                        old_count=old_count,
                        new_count=new_count,
                        old_min_time=None,
                        old_max_time=None,
                        new_min_time=None,
                        new_max_time=None,
                        first_mismatch=mismatch_reason,
                    )
                )

    return common_sources, diffs, old_only, new_only


def fmt_time_range(min_time: Optional[int], max_time: Optional[int]) -> str:
    if min_time is None and max_time is None:
        return "-"
    return f"{min_time or '-'}..{max_time or '-'}"


def read_varint(stream) -> int:
    shift = 0
    result = 0
    while True:
        raw = stream.read(1)
        if not raw:
            raise EOFError("unexpected EOF while reading uvarint")
        byte = raw[0]
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result
        shift += 7
        if shift >= 64:
            raise ValueError("uvarint too large")


def iter_old_records(path: Path, field: str) -> Iterator[Tuple[int, object]]:
    with path.open("rb") as handle:
        reader = zstd.ZstdDecompressor().stream_reader(handle)
        text_reader = io.TextIOWrapper(reader, encoding="utf-8")
        for raw_line in text_reader:
            text = raw_line.rstrip("\n")
            if not text:
                continue
            ts_text, payload_text = text.split("\t", 1)
            yield int(ts_text), json.loads(payload_text)


def iter_new_records(path: Path, field: str) -> Iterator[Tuple[int, object]]:
    with path.open("rb") as handle:
        reader = zstd.ZstdDecompressor().stream_reader(handle)
        header = reader.read(len(SPOOL_MAGIC))
        if header != SPOOL_MAGIC:
            raise ValueError(f"bad spool header: {path}")
        kind_raw = reader.read(1)
        if len(kind_raw) != 1:
            raise ValueError(f"missing spool kind: {path}")
        kind = kind_raw[0]

        while True:
            timestamp_raw = reader.read(8)
            if not timestamp_raw:
                break
            if len(timestamp_raw) != 8:
                raise EOFError(f"truncated timestamp in {path}")
            timestamp_ns = struct.unpack("<q", timestamp_raw)[0]
            size = read_varint(reader)
            payload = reader.read(size)
            if len(payload) != size:
                raise EOFError(f"truncated payload in {path}")

            if kind == SPOOL_KIND_STRING:
                yield timestamp_ns, payload.decode("utf-8")
            elif kind == SPOOL_KIND_INT64:
                if len(payload) != 8:
                    raise ValueError(f"bad int64 payload length {len(payload)} in {path}")
                yield timestamp_ns, struct.unpack("<q", payload)[0]
            else:
                raise ValueError(f"unknown spool kind {kind} in {path}")


def compare_record_streams(
    old_path: Path,
    new_path: Path,
    key: DiffKey,
    max_diffs: int,
) -> RecordDiff:
    old_iter = iter_old_records(old_path, key.field)
    new_iter = iter_new_records(new_path, key.field)

    old_count = 0
    new_count = 0
    old_min: Optional[int] = None
    old_max: Optional[int] = None
    new_min: Optional[int] = None
    new_max: Optional[int] = None
    first_mismatch: Optional[str] = None

    while True:
        try:
            old_item = next(old_iter)
            old_done = False
        except StopIteration:
            old_item = None
            old_done = True

        try:
            new_item = next(new_iter)
            new_done = False
        except StopIteration:
            new_item = None
            new_done = True

        if old_done and new_done:
            break
        if old_done != new_done:
            first_mismatch = first_mismatch or (
                f"length mismatch old_done={old_done} new_done={new_done} "
                f"old_count={old_count} new_count={new_count}"
            )
            break

        assert old_item is not None and new_item is not None
        old_count += 1
        new_count += 1

        old_ts, old_value = old_item
        new_ts, new_value = new_item
        old_min = old_ts if old_min is None else min(old_min, old_ts)
        old_max = old_ts if old_max is None else max(old_max, old_ts)
        new_min = new_ts if new_min is None else min(new_min, new_ts)
        new_max = new_ts if new_max is None else max(new_max, new_ts)

        if old_ts != new_ts or old_value != new_value:
            first_mismatch = first_mismatch or (
                f"record mismatch at row={old_count} "
                f"old=({old_ts},{truncate_value(old_value)}) "
                f"new=({new_ts},{truncate_value(new_value)})"
            )
            if max_diffs <= 1:
                break
            max_diffs -= 1

    return RecordDiff(
        key=key,
        old_count=old_count,
        new_count=new_count,
        old_min_time=old_min,
        old_max_time=old_max,
        new_min_time=new_min,
        new_max_time=new_max,
        first_mismatch=first_mismatch,
    )


def truncate_value(value: object, limit: int = 120) -> str:
    text = repr(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def main() -> int:
    args = parse_args()

    old_root = args.old_root
    new_root = args.new_root
    selected_sources = set(args.sources)
    selected_inst_ids = set(args.inst_ids)
    selected_fields = set(args.fields or FIELDS)

    old_manifests = discover_manifests(old_root)
    new_manifests = discover_manifests(new_root)

    common_sources, manifest_diffs, old_only, new_only = compare_manifest_level(
        old_root=old_root,
        new_root=new_root,
        old_manifests=old_manifests,
        new_manifests=new_manifests,
        selected_sources=selected_sources,
        selected_inst_ids=selected_inst_ids,
        selected_fields=selected_fields,
    )

    print("Manifest summary")
    print(
        f"old={len(old_manifests)} new={len(new_manifests)} "
        f"common={len(common_sources)} old_only={len(old_only)} new_only={len(new_only)}"
    )
    if old_only:
        print("old_only:", ",".join(old_only))
    if new_only:
        print("new_only:", ",".join(new_only))

    if manifest_diffs:
        print("\nManifest mismatches")
        for diff in manifest_diffs:
            print(
                f"{diff.key.source_id} {diff.key.inst_id} {diff.key.field}: "
                f"{diff.first_mismatch}"
            )
    else:
        print("\nManifest mismatches\nnone")

    record_diffs: List[RecordDiff] = []
    if args.verify_records:
        print("\nRecord verification")
        mismatch_keys = {diff.key for diff in manifest_diffs}
        for source_id in common_sources:
            old_manifest = old_manifests[source_id]
            new_manifest = new_manifests[source_id]
            for key in iter_selected_keys(source_id, old_manifest, new_manifest, selected_inst_ids, selected_fields):
                if args.verify_mismatch_only and key not in mismatch_keys:
                    continue
                old_path = manifest_path_for(key, old_root, "tsv.zst")
                new_path = manifest_path_for(key, new_root, "bin.zst")
                if not old_path.exists() or not new_path.exists():
                    record_diffs.append(
                        RecordDiff(
                            key=key,
                            old_count=0,
                            new_count=0,
                            old_min_time=None,
                            old_max_time=None,
                            new_min_time=None,
                            new_max_time=None,
                            first_mismatch=f"path old_exists={old_path.exists()} new_exists={new_path.exists()}",
                        )
                    )
                    continue
                diff = compare_record_streams(old_path, new_path, key, args.max_diffs)
                record_diffs.append(diff)
                if diff.first_mismatch:
                    print(
                        f"{key.source_id} {key.inst_id} {key.field}: {diff.first_mismatch}"
                    )
        if not record_diffs:
            print("none")

    any_manifest_diff = bool(manifest_diffs or old_only or new_only)
    any_record_diff = any(diff.first_mismatch for diff in record_diffs)
    return 1 if any_manifest_diff or any_record_diff else 0


if __name__ == "__main__":
    raise SystemExit(main())
