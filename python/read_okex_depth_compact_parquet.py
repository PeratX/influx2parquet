#!/usr/bin/env python3
"""Read the latest compact okex_depth parquet schema.

The current Go build stores asks/bids as packed binary columns in each row and
keeps the shared decimal scales in parquet key-value metadata.
This helper prints a small sample and can reconstruct asks/bids back into the
usual array-of-arrays text/JSON form.
"""

from __future__ import annotations

import argparse
import json
import struct
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
import pyarrow.parquet as pq


ACTION_NAMES = {
    1: "snapshot",
    2: "update",
}

PACKED_BOOK_VERSION = 1
PACKED_BOOK_HEADER_SIZE = 5
PACKED_BOOK_LEVEL_SIZE = 32


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read the latest okex_depth compact parquet datasets written by okex_depth_direct."
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
        help="Reconstruct compact asks/bids columns into readable arrays",
    )
    parser.add_argument(
        "--columns",
        nargs="+",
        default=[
            "time",
            "action",
            "asks",
            "asksRaw",
            "bids",
            "bidsRaw",
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


def load_dataset_metadata(dataset_path: Path) -> dict[str, str]:
    for part_path in sorted(dataset_path.glob("*.parquet")):
        metadata = pq.read_metadata(part_path).metadata or {}
        return {
            key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
            for key, value in metadata.items()
        }
    return {}


def maybe_decode_bytes(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    raise TypeError(f"unsupported binary payload type: {type(value)!r}")


def bytes_value(value: Any) -> bytes:
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, bytes):
        return value
    raise TypeError(f"unsupported binary payload type: {type(value)!r}")


def format_scaled_int(value: int, scale: int | None) -> str:
    if scale is None or scale <= 0:
        return str(value)
    sign = "-" if value < 0 else ""
    digits = str(abs(value))
    if len(digits) <= scale:
        digits = digits.rjust(scale + 1, "0")
    cut = len(digits) - scale
    whole = digits[:cut]
    frac = digits[cut:]
    frac = frac.rstrip("0")
    if not frac:
        return f"{sign}{whole}"
    return f"{sign}{whole}.{frac}"


def metadata_scale(metadata: dict[str, str], side: str, kind: str) -> int | None:
    value = metadata.get(f"okex_depth.{side.lower()}_{kind.lower()}_scale")
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def decode_packed_book(payload: bytes) -> list[tuple[int, int, int, int]]:
    if len(payload) < PACKED_BOOK_HEADER_SIZE:
        raise ValueError(f"packed book payload too short: {len(payload)}")
    if payload[0] != PACKED_BOOK_VERSION:
        raise ValueError(f"unsupported packed book version: {payload[0]}")
    level_count = int.from_bytes(payload[1:PACKED_BOOK_HEADER_SIZE], "little")
    expected_size = PACKED_BOOK_HEADER_SIZE + level_count * PACKED_BOOK_LEVEL_SIZE
    if len(payload) != expected_size:
        raise ValueError(
            f"packed book payload size mismatch: got={len(payload)} want={expected_size}"
        )
    body = memoryview(payload)[PACKED_BOOK_HEADER_SIZE:]
    return list(struct.iter_unpack("<qqqq", body))


def reconstruct_book_side(
    row: dict[str, Any], side: str, metadata: dict[str, str]
) -> tuple[str | None, list[list[str]] | None]:
    raw_key = f"{side}Raw"
    raw_payload = row.get(raw_key)
    if raw_payload is not None:
        text = maybe_decode_bytes(raw_payload)
        return text, json.loads(text)

    packed_payload = row.get(side)
    if packed_payload is None:
        return None, None

    px_scale = metadata_scale(metadata, side, "px")
    sz_scale = metadata_scale(metadata, side, "sz")
    levels = decode_packed_book(bytes_value(packed_payload))
    book: list[list[str]] = []
    for px_value, sz_value, liq_value, orders_value in levels:
        book.append(
            [
                format_scaled_int(px_value, px_scale),
                format_scaled_int(sz_value, sz_scale),
                str(liq_value),
                str(orders_value),
            ]
        )
    return json.dumps(book, ensure_ascii=False), book


def decorate_row(row: dict[str, Any], decode_book: bool, metadata: dict[str, str]) -> dict[str, Any]:
    output = dict(row)

    action = output.get("action")
    if action is not None:
        output["action_name"] = ACTION_NAMES.get(action, f"unknown({action})")

    if decode_book:
        for side in ("asks", "bids"):
            text, book = reconstruct_book_side(output, side, metadata)
            if text is not None:
                output[f"{side}_text"] = text
            if book is not None:
                output[f"{side}_json"] = book

    return output


def main() -> int:
    args = parse_args()
    rows = head_rows(args.dataset, args.limit, args.columns)
    metadata = load_dataset_metadata(args.dataset)
    print(
        json.dumps(
            {
                "dataset": str(args.dataset),
                "rows_returned": len(rows),
                "columns": args.columns,
                "parquet_metadata": metadata,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    for idx, row in enumerate(rows, start=1):
        decorated = decorate_row(row, args.decode_book, metadata)
        print(f"[row {idx}]")
        print(json.dumps(decorated, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
