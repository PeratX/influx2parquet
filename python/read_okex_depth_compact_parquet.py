#!/usr/bin/env python3
"""Read the latest okex_depth parquet schema.

The current Go build stores the original order-book JSON text directly in the
`asks` and `bids` columns, alongside `action`, `checksum`, and `ts`.
`instId` is dataset-level now and is implied by the dataset directory name.
This helper prints a small sample and can optionally parse `asks`/`bids` back
into Python lists.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read the latest okex_depth parquet datasets written by okex_depth_direct."
    )
    parser.add_argument(
        "dataset",
        type=Path,
        help="Path to a final dataset directory such as BTC-USDT-SWAP.parquet",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of rows to print, default: %(default)s",
    )
    parser.add_argument(
        "--decode-book",
        action="store_true",
        help="Parse asks/bids JSON text into arrays",
    )
    parser.add_argument(
        "--columns",
        nargs="+",
        default=[
            "time",
            "action",
            "asks",
            "bids",
            "checksum",
            "ts",
        ],
        help="Columns to read, default: %(default)s",
    )
    return parser.parse_args()


def head_rows(dataset_path: Path, limit: int, columns: list[str]) -> list[dict[str, Any]]:
    dataset = ds.dataset(str(dataset_path), format="parquet")
    existing = set(dataset.schema.names)
    selected = [column for column in columns if column in existing]
    scanner = dataset.scanner(columns=selected, batch_size=max(limit, 1))
    rows: list[dict[str, Any]] = []
    for batch in scanner.to_batches():
        rows.extend(batch.to_pylist())
        if len(rows) >= limit:
            return rows[:limit]
    return rows


def decode_book_side(text: Any) -> list[list[str]] | None:
    if text is None:
        return None
    if not isinstance(text, str):
        raise TypeError(f"unsupported asks/bids payload type: {type(text)!r}")
    return json.loads(text)


def decorate_row(row: dict[str, Any], decode_book: bool) -> dict[str, Any]:
    output = dict(row)
    if decode_book:
        for side in ("asks", "bids"):
            if side in output:
                book = decode_book_side(output.get(side))
                if book is not None:
                    output[f"{side}_json"] = book
    return output


def main() -> int:
    args = parse_args()
    rows = head_rows(args.dataset, args.limit, args.columns)
    print(
        json.dumps(
            {
                "dataset": str(args.dataset),
                "dataset_inst_id": args.dataset.stem.removesuffix(".parquet"),
                "rows_returned": len(rows),
                "columns": args.columns,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    for idx, row in enumerate(rows, start=1):
        decorated = decorate_row(row, args.decode_book)
        print(f"[row {idx}]")
        print(json.dumps(decorated, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
