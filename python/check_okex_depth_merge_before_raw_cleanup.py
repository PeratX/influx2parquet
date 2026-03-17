#!/usr/bin/env python3
from __future__ import annotations

import argparse
import heapq
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

try:
    import zstandard as zstd  # noqa: F401
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "missing dependency: zstandard\n"
        "run this script with /home/niko/influx2parquet/.venv/bin/python "
        "/home/niko/influx2parquet/python/check_okex_depth_merge_before_raw_cleanup.py"
    ) from exc

from compare_okex_depth_raw import FIELDS, iter_new_records, load_json


DEFAULT_STATE_PATH = Path("/mnt/backup_hdd/exported_parallel/okex_depth/_pipeline_state_go.json")
RAW_DIR_NAME = "_raw_go"
MERGED_DIR_NAME = "_merged_go"
BUILD_DIR_NAME = "_build_go"
MEASUREMENT = "okex_depth"
TSM_KIND = "tsm"


@dataclass(frozen=True)
class SourceFragment:
    source_id: str
    kind: str
    shard_id: int
    generation: int
    sequence: int
    raw_dir: str
    row_count: int


@dataclass
class ValidationIssue:
    level: str
    message: str


class FailedValidation(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate Go okex_depth merge outputs before manually deleting _raw_go. "
            "This script never deletes anything."
        )
    )
    parser.add_argument(
        "--state-path",
        type=Path,
        default=DEFAULT_STATE_PATH,
        help="path to _pipeline_state_go.json, default: %(default)s",
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        help="override raw root directory (normally inferred from state)",
    )
    parser.add_argument(
        "--merged-root",
        type=Path,
        help="override merged root directory (normally inferred from state)",
    )
    parser.add_argument(
        "--instid",
        action="append",
        dest="inst_ids",
        default=[],
        help="only validate selected instIds",
    )
    parser.add_argument(
        "--field",
        action="append",
        dest="fields",
        default=[],
        help="only validate selected fields",
    )
    parser.add_argument(
        "--verify-content",
        action="store_true",
        help=(
            "replay raw source fragments using the same merge ordering as Go code and "
            "compare every merged record; slower but strongest check"
        ),
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


def resolve_roots(args: argparse.Namespace, state_path: Path, state: dict) -> Tuple[Path, Path, Path, Path, Path]:
    meta = state.get("meta") or {}
    meta_root = Path(meta.get("meta_root") or state_path.parent)
    raw_root = Path(meta.get("raw_root") or meta_root / RAW_DIR_NAME)
    merged_root = Path(meta.get("merged_root") or meta_root / MERGED_DIR_NAME)
    build_root = Path(meta.get("build_root") or meta_root / BUILD_DIR_NAME)
    final_root = Path(meta.get("final_root") or meta_root)

    if args.raw_root:
        raw_root = default_measurement_root(args.raw_root) / RAW_DIR_NAME
    if args.merged_root:
        merged_root = default_measurement_root(args.merged_root) / MERGED_DIR_NAME

    return meta_root, raw_root, merged_root, build_root, final_root


def source_sort_key(fragment: SourceFragment) -> Tuple[int, int, int, int, str]:
    return (
        fragment.shard_id,
        0 if fragment.kind == TSM_KIND else 1,
        fragment.generation if fragment.kind == TSM_KIND else 0,
        fragment.sequence,
        fragment.source_id,
    )


def human_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(value)
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    if unit == "B":
        return f"{int(size)} {unit}"
    return f"{size:.2f} {unit}"


def directory_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for child in path.rglob("*"):
        if child.is_file():
            total += child.stat().st_size
    return total


def emit_issues(issues: Sequence[ValidationIssue], max_errors: int) -> None:
    if not issues:
        return
    for issue in issues[:max_errors]:
        print(f"[{issue.level}] {issue.message}")
    hidden = len(issues) - min(len(issues), max_errors)
    if hidden > 0:
        print(f"[error] ... and {hidden} more issues")


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


def merged_records_from_sources(source_paths: Sequence[Path], field: str) -> Iterator[Tuple[int, object]]:
    iterators: List[Iterator[Tuple[int, object]]] = [iter_collapsed_records(path, field) for path in source_paths]
    current: List[Optional[Tuple[int, object]]] = [None] * len(iterators)
    heap: List[Tuple[int, int, int]] = []

    for idx, iterator in enumerate(iterators):
        try:
            current[idx] = next(iterator)
        except StopIteration:
            continue
        timestamp_ns, _ = current[idx]
        heapq.heappush(heap, (timestamp_ns, idx, idx))

    while heap:
        timestamp_ns, rank, idx = heapq.heappop(heap)
        same = [(rank, idx)]
        while heap and heap[0][0] == timestamp_ns:
            _, next_rank, next_idx = heapq.heappop(heap)
            same.append((next_rank, next_idx))

        chosen_rank, chosen_idx = max(same, key=lambda item: item[0])
        chosen = current[chosen_idx]
        if chosen is None:
            raise FailedValidation(f"missing current record while replaying {field}")
        yield chosen

        for _, source_idx in same:
            try:
                current[source_idx] = next(iterators[source_idx])
            except StopIteration:
                current[source_idx] = None
                continue
            next_ts, _ = current[source_idx]
            heapq.heappush(heap, (next_ts, source_idx, source_idx))


def ensure(condition: bool, issues: List[ValidationIssue], message: str) -> None:
    if not condition:
        issues.append(ValidationIssue("error", message))


def validate_merge(
    state: dict,
    raw_root: Path,
    merged_root: Path,
    selected_inst_ids: set[str],
    selected_fields: set[str],
    verify_content: bool,
) -> Tuple[List[ValidationIssue], Dict[str, int]]:
    issues: List[ValidationIssue] = []
    stats: Dict[str, int] = {
        "complete_sources": 0,
        "merge_checks": 0,
        "merged_rows": 0,
        "raw_input_rows": 0,
    }

    files = state.get("files") or {}
    merge_state = state.get("merge") or {}
    meta = state.get("meta") or {}
    target_inst_ids = list(meta.get("inst_ids") or sorted(merge_state))
    if selected_inst_ids:
        target_inst_ids = [inst_id for inst_id in target_inst_ids if inst_id in selected_inst_ids]
    fields = [field for field in FIELDS if not selected_fields or field in selected_fields]

    fragments: Dict[str, Dict[str, List[SourceFragment]]] = {
        inst_id: {field: [] for field in fields}
        for inst_id in target_inst_ids
    }
    pending_sources: List[Tuple[str, str]] = []

    for source_id, entry in sorted(files.items()):
        status = entry.get("export_status")
        if status != "complete":
            pending_sources.append((source_id, str(status)))
            continue

        manifest_path = raw_root / source_id / "_manifest.json"
        ensure(manifest_path.exists(), issues, f"missing export manifest: {manifest_path}")
        if not manifest_path.exists():
            continue

        manifest = load_json(manifest_path)
        stats["complete_sources"] += 1

        manifest_records = int(manifest.get("records", 0))
        state_records = int(entry.get("export_records", 0))
        ensure(
            manifest_records == state_records,
            issues,
            f"export records mismatch for {source_id}: state={state_records} manifest={manifest_records}",
        )

        state_source = entry.get("source") or {}
        field_counts = manifest.get("field_record_counts") or {}
        raw_dir = Path(manifest.get("raw_dir") or (raw_root / source_id))
        for inst_id, field_map in field_counts.items():
            if inst_id not in fragments:
                continue
            for field, count in field_map.items():
                if field not in fields or int(count) <= 0:
                    continue
                raw_field_path = raw_dir / inst_id / f"{field}.bin.zst"
                ensure(raw_field_path.exists(), issues, f"missing raw fragment: {raw_field_path}")
                if not raw_field_path.exists():
                    continue
                fragments[inst_id][field].append(
                    SourceFragment(
                        source_id=source_id,
                        kind=str(state_source.get("kind") or ""),
                        shard_id=int(state_source.get("shard_id") or 0),
                        generation=int(state_source.get("generation") or 0),
                        sequence=int(state_source.get("sequence") or 0),
                        raw_dir=str(raw_dir),
                        row_count=int(count),
                    )
                )

    if pending_sources:
        sample = ", ".join(f"{source_id}:{status}" for source_id, status in pending_sources[:8])
        issues.append(
            ValidationIssue(
                "error",
                f"export not complete yet: complete={stats['complete_sources']} total={len(files)} "
                f"pending={len(pending_sources)} sample=[{sample}]",
            )
        )
        return issues, stats

    for inst_id in target_inst_ids:
        inst_merge = merge_state.get(inst_id) or {}
        for field in fields:
            stats["merge_checks"] += 1
            entry = inst_merge.get(field)
            ensure(entry is not None, issues, f"missing merge state: {inst_id}/{field}")
            if entry is None:
                continue

            status = entry.get("status")
            ensure(status == "complete", issues, f"merge incomplete: {inst_id}/{field} status={status}")
            if status != "complete":
                continue

            source_list = sorted(fragments[inst_id][field], key=source_sort_key)
            expected_source_ids = [item.source_id for item in source_list]
            expected_source_count = len(expected_source_ids)
            input_rows = sum(item.row_count for item in source_list)
            stats["raw_input_rows"] += input_rows

            actual_source_ids = list(entry.get("source_files") or [])
            ensure(
                actual_source_ids == expected_source_ids,
                issues,
                f"merge source_files mismatch for {inst_id}/{field}: "
                f"state={actual_source_ids[:3]}{'...' if len(actual_source_ids) > 3 else ''} "
                f"expected={expected_source_ids[:3]}{'...' if len(expected_source_ids) > 3 else ''}",
            )
            ensure(
                int(entry.get("source_count", -1)) == expected_source_count,
                issues,
                f"merge source_count mismatch for {inst_id}/{field}: "
                f"state={entry.get('source_count')} expected={expected_source_count}",
            )

            merged_path = Path(entry.get("dest_path") or (merged_root / inst_id / f"{field}.bin.zst"))
            expected_rows = int(entry.get("row_count", 0))

            if expected_source_count == 0:
                ensure(
                    expected_rows == 0,
                    issues,
                    f"zero-source field has non-zero rows for {inst_id}/{field}: {expected_rows}",
                )
                ensure(
                    not merged_path.exists(),
                    issues,
                    f"stale merged file exists for zero-source field {inst_id}/{field}: {merged_path}",
                )
                continue

            ensure(merged_path.exists(), issues, f"missing merged file: {merged_path}")
            if not merged_path.exists():
                continue

            merged_count = 0
            previous_ts: Optional[int] = None
            for timestamp_ns, _ in iter_new_records(merged_path, field):
                merged_count += 1
                if previous_ts is not None and timestamp_ns <= previous_ts:
                    issues.append(
                        ValidationIssue(
                            "error",
                            f"merged timestamps not strictly increasing for {inst_id}/{field}: "
                            f"{previous_ts} -> {timestamp_ns}",
                        )
                    )
                    break
                previous_ts = timestamp_ns

            stats["merged_rows"] += merged_count
            ensure(
                merged_count == expected_rows,
                issues,
                f"merge row_count mismatch for {inst_id}/{field}: state={expected_rows} actual={merged_count}",
            )
            ensure(
                merged_count <= input_rows,
                issues,
                f"merge rows exceed raw input for {inst_id}/{field}: merged={merged_count} raw={input_rows}",
            )

            if not verify_content:
                continue

            source_paths = [Path(item.raw_dir) / inst_id / f"{field}.bin.zst" for item in source_list]
            merged_iter = iter_new_records(merged_path, field)
            expected_iter = merged_records_from_sources(source_paths, field)
            row = 0
            while True:
                try:
                    merged_item = next(merged_iter)
                    merged_done = False
                except StopIteration:
                    merged_item = None
                    merged_done = True
                try:
                    expected_item = next(expected_iter)
                    expected_done = False
                except StopIteration:
                    expected_item = None
                    expected_done = True

                if merged_done and expected_done:
                    break
                row += 1
                ensure(
                    merged_done == expected_done,
                    issues,
                    f"merge replay length mismatch for {inst_id}/{field} at row={row}: "
                    f"merged_done={merged_done} expected_done={expected_done}",
                )
                if merged_done != expected_done:
                    break
                ensure(
                    merged_item == expected_item,
                    issues,
                    f"merge replay mismatch for {inst_id}/{field} at row={row}: "
                    f"merged={truncate_record(merged_item)} expected={truncate_record(expected_item)}",
                )
                if merged_item != expected_item:
                    break

    return issues, stats


def truncate_record(record: Optional[Tuple[int, object]], limit: int = 120) -> str:
    if record is None:
        return "None"
    timestamp_ns, value = record
    text = repr(value)
    if len(text) > limit:
        text = text[: limit - 3] + "..."
    return f"({timestamp_ns}, {text})"


def main() -> int:
    args = parse_args()
    state_path = args.state_path
    if not state_path.exists():
        raise SystemExit(f"state not found: {state_path}")

    state = load_json(state_path)
    meta_root, raw_root, merged_root, build_root, final_root = resolve_roots(args, state_path, state)
    selected_inst_ids = set(args.inst_ids)
    selected_fields = set(args.fields)

    print("[paths]")
    print(f"meta={meta_root}")
    print(f"raw={raw_root}")
    print(f"merged={merged_root}")
    print(f"build={build_root}")
    print(f"final={final_root}")
    if args.verify_content:
        print("[mode] deep verify enabled: replay raw fragments and compare merged content")
    else:
        print("[mode] fast verify: state/manifests/merged files only")

    issues, stats = validate_merge(
        state=state,
        raw_root=raw_root,
        merged_root=merged_root,
        selected_inst_ids=selected_inst_ids,
        selected_fields=selected_fields,
        verify_content=args.verify_content,
    )

    raw_bytes = directory_size(raw_root)
    merged_bytes = directory_size(merged_root)
    print("[summary]")
    print(f"complete_sources={stats['complete_sources']}")
    print(f"merge_checks={stats['merge_checks']}")
    print(f"raw_input_rows={stats['raw_input_rows']}")
    print(f"merged_rows={stats['merged_rows']}")
    print(f"raw_bytes={raw_bytes} ({human_bytes(raw_bytes)})")
    print(f"merged_bytes={merged_bytes} ({human_bytes(merged_bytes)})")

    if issues:
        print("[result] FAILED")
        emit_issues(issues, args.max_errors)
        return 1

    print("[result] OK")
    print(
        f"merge outputs are consistent with current state and merged files; "
        f"manual cleanup candidate: {raw_root} ({human_bytes(raw_bytes)})"
    )
    if not args.verify_content:
        print("tip: rerun with --verify-content before deleting raw if you want the strongest check")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
