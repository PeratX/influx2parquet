#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_EXPORTED_ROOT = Path("/mnt/backup_hdd/exported")
DEFAULT_PARALLEL_ROOT = Path("/mnt/backup_hdd/exported_parallel")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a compact JSON catalog for exported and exported_parallel, suitable for LLM context."
    )
    parser.add_argument("--exported-root", type=Path, default=DEFAULT_EXPORTED_ROOT)
    parser.add_argument("--parallel-root", type=Path, default=DEFAULT_PARALLEL_ROOT)
    parser.add_argument(
        "--section",
        choices=["all", "exported", "parallel"],
        default="all",
        help="limit output to one section",
    )
    parser.add_argument("--pretty", action="store_true", help="pretty-print JSON")
    return parser.parse_args()


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def safe_read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None


def catalog_exported(root: Path) -> Dict[str, Any]:
    datasets: List[Dict[str, Any]] = []
    if not root.exists():
        return {"root": str(root), "datasets": datasets}

    for dataset_dir in sorted(p for p in root.iterdir() if p.is_dir() and p.name.endswith(".parquet")):
        entry: Dict[str, Any] = {
            "name": dataset_dir.name,
            "path": str(dataset_dir),
            "type": "final_parquet_dataset",
        }

        summary_path = dataset_dir / "_summary.json"
        if summary_path.exists():
            summary = load_json(summary_path)
            entry["measurement"] = summary.get("measurement")
            entry["record_count"] = summary.get("record_count")
            entry["part_count"] = summary.get("part_count")
            entry["dataset_size_human"] = summary.get("dataset_size_human")
            entry["dataset_size_bytes"] = summary.get("dataset_size_bytes")
            entry["date_range"] = summary.get("date_range")
            entry["compression"] = summary.get("compression")
            entry["table_format"] = summary.get("table_format")

        description_path = dataset_dir / "_description.md"
        description = safe_read_text(description_path)
        if description:
            first_line = next((line.strip() for line in description.splitlines() if line.strip()), None)
            if first_line:
                entry["description_first_line"] = first_line

        datasets.append(entry)

    return {"root": str(root), "datasets": datasets}


def summarize_status_counts(items: Dict[str, Any], key: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for value in items.values():
        status = value.get(key)
        counts[status] = counts.get(status, 0) + 1
    return counts


def summarize_nested_status_counts(items: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for per_inst in items.values():
        for value in per_inst.values():
            status = value.get("status")
            counts[status] = counts.get(status, 0) + 1
    return counts


def catalog_parallel(root: Path) -> Dict[str, Any]:
    result: Dict[str, Any] = {"root": str(root)}
    if not root.exists():
        return result

    depth20_dir = root / "binance_depth20"
    if depth20_dir.exists():
        depth20_entry: Dict[str, Any] = {
            "path": str(depth20_dir),
            "type": "parallel_final_datasets",
        }
        manifest_path = depth20_dir / "_manifest.json"
        if manifest_path.exists():
            manifest = load_json(manifest_path)
            tasks = manifest.get("tasks", [])
            depth20_entry["measurement"] = manifest.get("measurement")
            depth20_entry["task_count"] = len(tasks)
            depth20_entry["status_counts"] = _count_task_status(tasks)
            depth20_entry["datasets"] = [
                {
                    "inst_id": task.get("inst_id"),
                    "status": task.get("status"),
                    "dataset_path": task.get("dataset_path"),
                    "log_path": task.get("log_path"),
                }
                for task in tasks
            ]
        result["binance_depth20"] = depth20_entry

    okex_dir = root / "okex_depth"
    if okex_dir.exists():
        okex_entry: Dict[str, Any] = {
            "path": str(okex_dir),
            "type": "resumable_pipeline_workspace",
            "has_raw_old": (okex_dir / "_raw").exists(),
            "has_raw_backup": (okex_dir / "_raw.bak").exists(),
            "has_raw_go": (okex_dir / "_raw_go").exists(),
            "has_merged_old": (okex_dir / "_merged").exists(),
            "has_merged_go": (okex_dir / "_merged_go").exists(),
            "has_build_old": (okex_dir / "_build").exists(),
            "has_build_go": (okex_dir / "_build_go").exists(),
            "final_datasets": sorted(p.name for p in okex_dir.glob("*.parquet") if p.is_dir()),
        }

        scan_path = okex_dir / "_tsm_index.json"
        if scan_path.exists():
            scan = load_json(scan_path)
            okex_entry["scan"] = {
                "scan_complete": scan.get("scan_complete"),
                "matched_tsm_files": scan.get("matched_tsm_files"),
                "matched_tsm_bytes": scan.get("matched_tsm_bytes"),
                "inst_ids": scan.get("inst_ids"),
                "data_layout": scan.get("data_layout"),
                "retention_root": scan.get("retention_root"),
            }

        py_state_path = okex_dir / "_pipeline_state.json"
        if py_state_path.exists():
            py_state = load_json(py_state_path)
            files = py_state.get("files", {})
            merge = py_state.get("merge", {})
            build = py_state.get("build", {})
            okex_entry["python_pipeline"] = {
                "file_status_counts": summarize_status_counts(files, "export_status"),
                "merge_status_counts": summarize_nested_status_counts(merge),
                "build_status_counts": summarize_status_counts(build, "status"),
            }

        go_state_path = okex_dir / "_pipeline_state_go.json"
        if go_state_path.exists():
            go_state = load_json(go_state_path)
            files = go_state.get("files", {})
            merge = go_state.get("merge", {})
            build = go_state.get("build", {})
            okex_entry["go_pipeline"] = {
                "meta": go_state.get("meta", {}),
                "file_status_counts": summarize_status_counts(files, "export_status"),
                "merge_status_counts": summarize_nested_status_counts(merge),
                "build_status_counts": summarize_status_counts(build, "status"),
            }

        result["okex_depth"] = okex_entry

    return result


def _count_task_status(tasks: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for task in tasks:
        status = task.get("status")
        counts[status] = counts.get(status, 0) + 1
    return counts


def main() -> int:
    args = parse_args()

    output: Dict[str, Any] = {}
    if args.section in ("all", "exported"):
        output["exported"] = catalog_exported(args.exported_root)
    if args.section in ("all", "parallel"):
        output["exported_parallel"] = catalog_parallel(args.parallel_root)

    if args.pretty:
        json.dump(output, fp=sys.stdout, ensure_ascii=False, indent=2)
        print()
    else:
        print(json.dumps(output, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
