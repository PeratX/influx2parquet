#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "missing dependency: pyarrow\n"
        "run this script with /home/niko/influx2parquet/.venv/bin/python "
        "/home/niko/influx2parquet/python/check_okex_depth_build_against_merged.py"
    ) from exc

from compare_okex_depth_raw import FIELDS, iter_new_records, load_json


DEFAULT_STATE_PATH = Path("/mnt/backup_hdd/exported_parallel/okex_depth/_pipeline_state_go.json")
MEASUREMENT = "okex_depth"
MERGED_DIR_NAME = "_merged_go"
PACKED_BOOK_VERSION = 1
PACKED_BOOK_HEADER_SIZE = 5
PACKED_BOOK_LEVEL_SIZE = 32


@dataclass
class ValidationIssue:
    level: str
    message: str


@dataclass(frozen=True)
class CanonicalRow:
    time_ns: int
    action: Optional[int]
    asks_raw: Optional[bytes]
    asks: Optional[bytes]
    bids_raw: Optional[bytes]
    bids: Optional[bytes]
    checksum: Optional[int]
    ts: Optional[int]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate final okex_depth parquet datasets against merged spool files. "
            "By default this checks parquet row counts and the first N rows; use --verify-content "
            "for a full row-by-row comparison."
        )
    )
    parser.add_argument(
        "--state-path",
        type=Path,
        default=DEFAULT_STATE_PATH,
        help="path to _pipeline_state_go.json, default: %(default)s",
    )
    parser.add_argument(
        "--merged-root",
        type=Path,
        help="override merged root directory (normally inferred from state)",
    )
    parser.add_argument(
        "--final-root",
        type=Path,
        help="override final dataset root (normally inferred from state)",
    )
    parser.add_argument(
        "--instid",
        action="append",
        dest="inst_ids",
        default=[],
        help="only validate selected instIds",
    )
    parser.add_argument(
        "--sample-verify",
        type=int,
        default=20,
        help="number of leading rows per instId to compare, default: %(default)s",
    )
    parser.add_argument(
        "--verify-content",
        action="store_true",
        help="compare every reconstructed merged row against parquet rows",
    )
    parser.add_argument(
        "--max-errors",
        type=int,
        default=50,
        help="max detailed error lines to print before stopping, default: %(default)s",
    )
    return parser.parse_args()


def default_measurement_root(base: Path) -> Path:
    return base / MEASUREMENT


def resolve_roots(args: argparse.Namespace, state_path: Path, state: dict) -> Tuple[Path, Path, Path]:
    meta = state.get("meta") or {}
    meta_root = Path(meta.get("meta_root") or state_path.parent)
    merged_root = Path(meta.get("merged_root") or meta_root / MERGED_DIR_NAME)
    final_root = Path(meta.get("final_root") or meta_root)

    if args.merged_root:
        merged_root = default_measurement_root(args.merged_root) / MERGED_DIR_NAME
    if args.final_root:
        final_root = default_measurement_root(args.final_root)

    return meta_root, merged_root, final_root


def choose_sample_targets(total_rows: int, sample_count: int) -> set[int]:
    if total_rows <= 0 or sample_count <= 0:
        return set()
    return set(range(1, min(total_rows, sample_count) + 1))


def emit_issues(issues: Sequence[ValidationIssue], max_errors: int) -> None:
    if not issues:
        return
    for issue in issues[:max_errors]:
        print(f"[{issue.level}] {issue.message}")
    hidden = len(issues) - min(len(issues), max_errors)
    if hidden > 0:
        print(f"[error] ... and {hidden} more issues")


def ensure(condition: bool, issues: List[ValidationIssue], message: str) -> None:
    if not condition:
        issues.append(ValidationIssue("error", message))


def metadata_scale(metadata: dict[str, str], side: str, kind: str) -> Optional[int]:
    value = metadata.get(f"okex_depth.{side.lower()}_{kind.lower()}_scale")
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def load_dataset_metadata(dataset_path: Path) -> dict[str, str]:
    for part_path in sorted(dataset_path.glob("part-*.parquet")):
        metadata = pq.read_metadata(part_path).metadata or {}
        return {
            key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
            for key, value in metadata.items()
        }
    return {}


def parquet_row_count(dataset_path: Path) -> int:
    total_rows = 0
    for part_path in sorted(dataset_path.glob("part-*.parquet")):
        total_rows += pq.read_metadata(part_path).num_rows
    return total_rows


def detect_inst_ids(state: dict, selected_inst_ids: set[str]) -> List[str]:
    meta = state.get("meta") or {}
    build_state = state.get("build") or {}
    merge_state = state.get("merge") or {}

    target_inst_ids = list(meta.get("inst_ids") or sorted(build_state) or sorted(merge_state))
    if selected_inst_ids:
        target_inst_ids = [inst_id for inst_id in target_inst_ids if inst_id in selected_inst_ids]
    return target_inst_ids


def iter_collapsed_records(path: Path, field: str) -> Iterator[Tuple[int, object]]:
    iterator = iter_new_records(path, field)
    try:
        current_ts, current_value = next(iterator)
    except StopIteration:
        return
    for timestamp_ns, value in iterator:
        if timestamp_ns != current_ts:
            yield current_ts, current_value
            current_ts, current_value = timestamp_ns, value
            continue
        current_value = value
    yield current_ts, current_value


def next_timestamp(current: Dict[str, Optional[Tuple[int, object]]]) -> int:
    present = [item[0] for item in current.values() if item is not None]
    if not present:
        raise RuntimeError("no current merged records")
    return min(present)


def format_scaled_int(value: int, scale: Optional[int]) -> str:
    if scale is None or scale <= 0:
        return str(value)
    sign = "-" if value < 0 else ""
    digits = str(abs(value))
    if len(digits) <= scale:
        digits = digits.rjust(scale + 1, "0")
    cut = len(digits) - scale
    whole = digits[:cut]
    frac = digits[cut:].rstrip("0")
    if not frac:
        return f"{sign}{whole}"
    return f"{sign}{whole}.{frac}"


def decode_packed_book(payload: bytes) -> List[Tuple[int, int, int, int]]:
    if len(payload) < PACKED_BOOK_HEADER_SIZE:
        raise ValueError(f"packed book payload too short: {len(payload)}")
    if payload[0] != PACKED_BOOK_VERSION:
        raise ValueError(f"unsupported packed book version: {payload[0]}")
    level_count = int.from_bytes(payload[1:PACKED_BOOK_HEADER_SIZE], "little")
    expected_size = PACKED_BOOK_HEADER_SIZE + level_count * PACKED_BOOK_LEVEL_SIZE
    if len(payload) != expected_size:
        raise ValueError(f"packed book payload size mismatch: got={len(payload)} want={expected_size}")
    body = memoryview(payload)[PACKED_BOOK_HEADER_SIZE:]
    return list(struct.iter_unpack("<qqqq", body))


def decode_book_preview(payload: bytes, side: str, metadata: dict[str, str], limit: int = 2) -> str:
    px_scale = metadata_scale(metadata, side, "px")
    sz_scale = metadata_scale(metadata, side, "sz")
    levels = decode_packed_book(payload)
    book = [
        [
            format_scaled_int(px_value, px_scale),
            format_scaled_int(sz_value, sz_scale),
            str(liq_value),
            str(orders_value),
        ]
        for px_value, sz_value, liq_value, orders_value in levels[:limit]
    ]
    suffix = "" if len(levels) <= limit else f"...(+{len(levels) - limit} levels)"
    return json.dumps(book, ensure_ascii=False) + suffix


def parse_scaled_decimal_text(text: object, scale: int) -> int:
    body = str(text).strip()
    if not body:
        raise ValueError("empty decimal")

    sign = 1
    if body[0] == "+":
        body = body[1:]
    elif body[0] == "-":
        sign = -1
        body = body[1:]
    if not body:
        raise ValueError("invalid decimal")

    if "." in body:
        whole, frac = body.split(".", 1)
    else:
        whole, frac = body, ""
    if not whole and not frac:
        raise ValueError("invalid decimal")
    if whole and not whole.isdigit():
        raise ValueError(f"invalid whole digits: {whole!r}")
    if frac and not frac.isdigit():
        raise ValueError(f"invalid fractional digits: {frac!r}")

    frac = frac.rstrip("0")
    if len(frac) > scale:
        raise ValueError(f"decimal {text!r} exceeds scale {scale}")

    digits = (whole or "0") + frac
    value = int(digits or "0")
    value *= 10 ** (scale - len(frac))
    return sign * value


def pack_book_payload(payload_text: str, px_scale: int, sz_scale: int) -> bytes:
    levels = json.loads(payload_text)
    if not isinstance(levels, list):
        raise ValueError("book payload is not a list")

    body = bytearray()
    for level in levels:
        if not isinstance(level, list) or len(level) != 4:
            raise ValueError(f"invalid book level: {level!r}")
        px = parse_scaled_decimal_text(level[0], px_scale)
        sz = parse_scaled_decimal_text(level[1], sz_scale)
        liq = int(str(level[2]))
        orders = int(str(level[3]))
        body.extend(struct.pack("<qqqq", px, sz, liq, orders))

    return bytes([PACKED_BOOK_VERSION]) + struct.pack("<I", len(levels)) + bytes(body)


def build_expected_row(
    time_ns: int,
    field_payloads: Dict[str, object],
    metadata: dict[str, str],
) -> CanonicalRow:
    row = CanonicalRow(
        time_ns=time_ns,
        action=None,
        asks_raw=None,
        asks=None,
        bids_raw=None,
        bids=None,
        checksum=None,
        ts=None,
    )

    action = row.action
    asks_raw = row.asks_raw
    asks = row.asks
    bids_raw = row.bids_raw
    bids = row.bids
    checksum = row.checksum
    ts = row.ts

    if "action" in field_payloads:
        action_text = str(field_payloads["action"])
        if action_text == "snapshot":
            action = 1
        elif action_text == "update":
            action = 2
        else:
            raise ValueError(f"unknown action payload: {action_text!r}")

    for side in ("asks", "bids"):
        if side not in field_payloads:
            continue
        payload_text = str(field_payloads[side])
        px_scale = metadata_scale(metadata, side, "px")
        sz_scale = metadata_scale(metadata, side, "sz")
        if px_scale is None or sz_scale is None:
            raise ValueError(f"missing metadata scale for {side}")
        try:
            packed = pack_book_payload(payload_text, px_scale, sz_scale)
        except Exception:
            packed = None
        if side == "asks":
            asks = packed
            asks_raw = payload_text.encode("utf-8") if packed is None else None
        else:
            bids = packed
            bids_raw = payload_text.encode("utf-8") if packed is None else None

    if "checksum" in field_payloads:
        checksum = int(field_payloads["checksum"])
    if "ts" in field_payloads:
        ts = int(str(field_payloads["ts"]))

    return CanonicalRow(
        time_ns=time_ns,
        action=action,
        asks_raw=asks_raw,
        asks=asks,
        bids_raw=bids_raw,
        bids=bids,
        checksum=checksum,
        ts=ts,
    )


def iter_expected_rows(
    inst_id: str,
    merge_entry: dict,
    metadata: dict[str, str],
    merged_root: Path,
) -> Iterator[CanonicalRow]:
    iterators: Dict[str, Iterator[Tuple[int, object]]] = {}
    current: Dict[str, Optional[Tuple[int, object]]] = {}

    for field in FIELDS:
        entry = (merge_entry.get(field) or {})
        if int(entry.get("source_count") or 0) <= 0:
            continue
        merged_path = Path(entry.get("dest_path") or (merged_root / inst_id / f"{field}.bin.zst"))
        if not merged_path.exists():
            raise FileNotFoundError(f"missing merged file for {inst_id}/{field}: {merged_path}")
        iterator = iter_collapsed_records(merged_path, field)
        iterators[field] = iterator
        current[field] = next(iterator, None)

    while any(item is not None for item in current.values()):
        time_ns = next_timestamp(current)
        payloads: Dict[str, object] = {}
        for field, item in list(current.items()):
            if item is None or item[0] != time_ns:
                continue
            payloads[field] = item[1]
            current[field] = next(iterators[field], None)
        yield build_expected_row(time_ns, payloads, metadata)


def scalar_bytes(value: object) -> Optional[bytes]:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
    raise TypeError(f"unsupported bytes payload type: {type(value)!r}")


def iter_parquet_rows(dataset_path: Path) -> Iterator[CanonicalRow]:
    columns = ["time", "action", "asksRaw", "asks", "bidsRaw", "bids", "checksum", "ts"]
    part_paths = sorted(dataset_path.glob("part-*.parquet"))
    for part_path in part_paths:
        parquet_file = pq.ParquetFile(part_path)
        for batch in parquet_file.iter_batches(columns=columns):
            names = batch.schema.names
            arrays = {name: batch.column(names.index(name)) for name in names}
            time_array = pc.cast(arrays["time"], pa.int64())
            for index in range(batch.num_rows):
                yield CanonicalRow(
                    time_ns=int(time_array[index].as_py()),
                    action=arrays["action"][index].as_py() if "action" in arrays else None,
                    asks_raw=scalar_bytes(arrays["asksRaw"][index].as_py()) if "asksRaw" in arrays else None,
                    asks=scalar_bytes(arrays["asks"][index].as_py()) if "asks" in arrays else None,
                    bids_raw=scalar_bytes(arrays["bidsRaw"][index].as_py()) if "bidsRaw" in arrays else None,
                    bids=scalar_bytes(arrays["bids"][index].as_py()) if "bids" in arrays else None,
                    checksum=arrays["checksum"][index].as_py() if "checksum" in arrays else None,
                    ts=arrays["ts"][index].as_py() if "ts" in arrays else None,
                )


def truncate_bytes(value: Optional[bytes], limit: int = 24) -> str:
    if value is None:
        return "None"
    if len(value) <= limit:
        return value.hex()
    return value[:limit].hex() + "..."


def truncate_row(row: Optional[CanonicalRow], metadata: dict[str, str], limit: int = 2) -> str:
    if row is None:
        return "None"
    parts = [f"time={row.time_ns}", f"action={row.action}", f"checksum={row.checksum}", f"ts={row.ts}"]
    if row.asks_raw is not None:
        parts.append(f"asksRaw={row.asks_raw.decode('utf-8', errors='replace')[:120]}")
    elif row.asks is not None:
        try:
            preview = decode_book_preview(row.asks, "asks", metadata, limit)
        except Exception:
            preview = f"<decode-error {truncate_bytes(row.asks)}>"
        parts.append(f"asks={preview}")
    else:
        parts.append("asks=None")
    if row.bids_raw is not None:
        parts.append(f"bidsRaw={row.bids_raw.decode('utf-8', errors='replace')[:120]}")
    elif row.bids is not None:
        try:
            preview = decode_book_preview(row.bids, "bids", metadata, limit)
        except Exception:
            preview = f"<decode-error {truncate_bytes(row.bids)}>"
        parts.append(f"bids={preview}")
    else:
        parts.append("bids=None")
    return "{" + ", ".join(parts) + "}"


def collect_sample_rows(
    iterator: Iterator[CanonicalRow],
    sample_targets: set[int],
) -> Tuple[int, Dict[int, CanonicalRow]]:
    samples: Dict[int, CanonicalRow] = {}
    total_rows = 0
    for total_rows, row in enumerate(iterator, start=1):
        if total_rows in sample_targets:
            samples[total_rows] = row
    return total_rows, samples


def collect_leading_rows(iterator: Iterator[CanonicalRow], limit: int) -> Dict[int, CanonicalRow]:
    samples: Dict[int, CanonicalRow] = {}
    if limit <= 0:
        return samples
    for row_number, row in enumerate(iterator, start=1):
        if row_number > limit:
            break
        samples[row_number] = row
    return samples


def compare_all_rows(
    inst_id: str,
    expected_iter: Iterator[CanonicalRow],
    actual_iter: Iterator[CanonicalRow],
    metadata: dict[str, str],
    issues: List[ValidationIssue],
) -> Tuple[int, int]:
    expected_count = 0
    actual_count = 0
    while True:
        try:
            expected_row = next(expected_iter)
            expected_done = False
            expected_count += 1
        except StopIteration:
            expected_row = None
            expected_done = True

        try:
            actual_row = next(actual_iter)
            actual_done = False
            actual_count += 1
        except StopIteration:
            actual_row = None
            actual_done = True

        if expected_done and actual_done:
            break

        if expected_done != actual_done:
            issues.append(
                ValidationIssue(
                    "error",
                    f"row length mismatch for {inst_id}: expected_done={expected_done} actual_done={actual_done} "
                    f"expected_count={expected_count} actual_count={actual_count}",
                )
            )
            break

        assert expected_row is not None and actual_row is not None
        if expected_row != actual_row:
            issues.append(
                ValidationIssue(
                    "error",
                    f"row mismatch for {inst_id} at row={expected_count}: "
                    f"expected={truncate_row(expected_row, metadata)} "
                    f"actual={truncate_row(actual_row, metadata)}",
                )
            )
            break

    return expected_count, actual_count


def validate_build(
    state: dict,
    merged_root: Path,
    final_root: Path,
    selected_inst_ids: set[str],
    sample_verify: int,
    verify_content: bool,
) -> Tuple[List[ValidationIssue], Dict[str, int]]:
    issues: List[ValidationIssue] = []
    stats: Dict[str, int] = {
        "datasets_checked": 0,
        "state_rows": 0,
        "merged_rows": 0,
        "parquet_rows": 0,
        "sample_rows_compared": 0,
    }

    merge_state = state.get("merge") or {}
    build_state = state.get("build") or {}
    target_inst_ids = detect_inst_ids(state, selected_inst_ids)

    for inst_id in target_inst_ids:
        stats["datasets_checked"] += 1

        build_entry = build_state.get(inst_id) or {}
        status = build_entry.get("status")
        ensure(status == "complete", issues, f"build incomplete for {inst_id}: status={status}")
        if status != "complete":
            continue

        merge_entry = merge_state.get(inst_id) or {}
        for field in FIELDS:
            field_entry = merge_entry.get(field) or {}
            if int(field_entry.get("source_count") or 0) <= 0:
                continue
            merged_path = Path(field_entry.get("dest_path") or (merged_root / inst_id / f"{field}.bin.zst"))
            ensure(merged_path.exists(), issues, f"missing merged file for {inst_id}/{field}: {merged_path}")

        dataset_path = final_root / f"{inst_id}.parquet"
        ensure(dataset_path.exists(), issues, f"missing parquet dataset for {inst_id}: {dataset_path}")
        if not dataset_path.exists():
            continue

        metadata = load_dataset_metadata(dataset_path)
        ensure(metadata.get("okex_depth.inst_id") == inst_id, issues, f"parquet metadata inst_id mismatch for {inst_id}")
        ensure(
            metadata.get("okex_depth.book_encoding") == "packed_i64le_v1",
            issues,
            f"unexpected book encoding for {inst_id}: {metadata.get('okex_depth.book_encoding')}",
        )

        expected_rows = int(build_entry.get("rows") or 0)
        stats["state_rows"] += expected_rows

        if verify_content:
            merged_count, parquet_count = compare_all_rows(
                inst_id=inst_id,
                expected_iter=iter_expected_rows(inst_id, merge_entry, metadata, merged_root),
                actual_iter=iter_parquet_rows(dataset_path),
                metadata=metadata,
                issues=issues,
            )
            stats["merged_rows"] += merged_count
            stats["parquet_rows"] += parquet_count
            ensure(
                merged_count == expected_rows,
                issues,
                f"merged row count mismatch for {inst_id}: state={expected_rows} rebuilt={merged_count}",
            )
            ensure(
                parquet_count == expected_rows,
                issues,
                f"parquet row count mismatch for {inst_id}: state={expected_rows} parquet={parquet_count}",
            )
            continue

        sample_targets = choose_sample_targets(expected_rows, sample_verify)
        rebuilt_samples = collect_leading_rows(iter_expected_rows(inst_id, merge_entry, metadata, merged_root), len(sample_targets))
        parquet_count = parquet_row_count(dataset_path)
        parquet_samples = collect_leading_rows(iter_parquet_rows(dataset_path), len(sample_targets))
        stats["merged_rows"] += len(rebuilt_samples)
        stats["parquet_rows"] += parquet_count
        stats["sample_rows_compared"] += len(sample_targets)

        ensure(
            parquet_count == expected_rows,
            issues,
            f"parquet row count mismatch for {inst_id}: state={expected_rows} parquet={parquet_count}",
        )
        ensure(
            set(rebuilt_samples) == sample_targets,
            issues,
            f"rebuilt sample rows missing for {inst_id}: expected={sorted(sample_targets)} actual={sorted(rebuilt_samples)}",
        )
        ensure(
            set(parquet_samples) == sample_targets,
            issues,
            f"parquet sample rows missing for {inst_id}: expected={sorted(sample_targets)} actual={sorted(parquet_samples)}",
        )

        for row_number in sorted(sample_targets):
            expected_row = rebuilt_samples.get(row_number)
            actual_row = parquet_samples.get(row_number)
            ensure(
                expected_row == actual_row,
                issues,
                f"sample row mismatch for {inst_id} at row={row_number}: "
                f"expected={truncate_row(expected_row, metadata)} "
                f"actual={truncate_row(actual_row, metadata)}",
            )

    return issues, stats


def main() -> int:
    args = parse_args()
    state_path = args.state_path
    if not state_path.exists():
        raise SystemExit(f"state not found: {state_path}")

    state = load_json(state_path)
    meta_root, merged_root, final_root = resolve_roots(args, state_path, state)
    selected_inst_ids = set(args.inst_ids)

    print("[paths]")
    print(f"meta={meta_root}")
    print(f"merged={merged_root}")
    print(f"final={final_root}")
    print(f"mode={'full' if args.verify_content else 'sample'}")
    if not args.verify_content:
        print(f"sample_verify={args.sample_verify}")

    issues, stats = validate_build(
        state=state,
        merged_root=merged_root,
        final_root=final_root,
        selected_inst_ids=selected_inst_ids,
        sample_verify=args.sample_verify,
        verify_content=args.verify_content,
    )

    print("\n[stats]")
    for key, value in stats.items():
        print(f"{key}={value}")

    if issues:
        print("\n[issues]")
        emit_issues(issues, args.max_errors)
        return 1

    print("\n[ok] merged rebuild matches final parquet")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
