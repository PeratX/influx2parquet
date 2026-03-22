from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))

import compare_parquet_sizes as compare_sizes


def write_minimal_part(dataset_dir: Path) -> None:
    table = pa.table({"value": [1, 2, 3]})
    pq.write_table(table, dataset_dir / "part-000000.parquet")


class CompareParquetSizesTests(unittest.TestCase):
    def test_okex_depth_measurement_root_uses_summary_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "okex_depth"
            dataset_dir = root / "BTC-USDT-SWAP.parquet"
            dataset_dir.mkdir(parents=True)
            write_minimal_part(dataset_dir)
            (dataset_dir / "summary.json").write_text(
                json.dumps({"measurement": "okex_depth"}),
                encoding="utf-8",
            )

            measurement, dataset = compare_sizes.derive_measurement(root, dataset_dir)
            self.assertEqual(measurement, "okex_depth")
            self.assertEqual(dataset, "BTC-USDT-SWAP.parquet")
            self.assertEqual(compare_sizes.dataset_status(dataset_dir), "completed")

    def test_deribit_option_measurement_root_uses_underscore_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "deribit_option"
            dataset_dir = root / "ETH-SEP22.parquet"
            dataset_dir.mkdir(parents=True)
            write_minimal_part(dataset_dir)
            (dataset_dir / "_summary.json").write_text(
                json.dumps({"measurement": "deribit_option"}),
                encoding="utf-8",
            )

            measurement, dataset = compare_sizes.derive_measurement(root, dataset_dir)
            self.assertEqual(measurement, "deribit_option")
            self.assertEqual(dataset, "ETH-SEP22.parquet")
            self.assertEqual(compare_sizes.dataset_status(dataset_dir), "completed")


if __name__ == "__main__":
    unittest.main()
