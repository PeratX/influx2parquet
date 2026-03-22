from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))

import extract_optiondata_to_parquet as exporter


class FakeInfluxClient:
    def __init__(self) -> None:
        self._payloads = [
            {
                "results": [
                    {
                        "series": [
                            {
                                "columns": ["time", "instrument", "mark_price"],
                                "values": [
                                    [1, "ETH-9SEP22-1000-C", 1.1],
                                    [2, "ETH-9SEP22-1000-C", 1.2],
                                ],
                            }
                        ]
                    }
                ]
            },
            {
                "results": [
                    {
                        "series": [
                            {
                                "columns": ["time", "instrument", "mark_price"],
                                "values": [
                                    [3, "ETH-16SEP22-1000-C", 1.3],
                                    [4, "ETH-16SEP22-1000-C", 1.4],
                                ],
                            }
                        ]
                    }
                ]
            },
        ]

    def field_types(self, measurement: str) -> dict[str, str]:
        assert measurement == "deribit_option"
        return {"mark_price": "float"}

    def tag_keys(self, measurement: str) -> list[str]:
        assert measurement == "deribit_option"
        return ["instrument"]

    def show_series_keys(self, measurement: str) -> list[str]:
        assert measurement == "deribit_option"
        return [
            "deribit_option,instrument=ETH-16SEP22-1000-C",
            "deribit_option,instrument=ETH-9SEP22-1000-C",
        ]

    def query_chunks(self, query: str, chunk_size: int):
        assert "SELECT * FROM \"deribit_option\"" in query
        assert chunk_size == 1000
        yield self._payloads.pop(0)


class ExportOptionDataTests(unittest.TestCase):
    def test_grouped_series_buffering_can_span_multiple_instruments(self) -> None:
        client = FakeInfluxClient()
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_path = Path(temp_dir) / "ETH-SEP22.parquet"
            dataset_path.mkdir()
            checkpoint = exporter.Checkpoint(
                measurement="deribit_option",
                database="OptionData",
                dataset_path=str(dataset_path),
                start=None,
                end=None,
                limit=None,
                chunk_size=1000,
                rows_per_file=10,
                compression="zstd",
                compression_level=1,
                series_key=None,
                series_tag_key="instrument",
                series_tag_regex=r"^ETH-[0-9]{1,2}SEP22-",
                rows_per_file_history=[10],
            )

            result = exporter.export_measurement(
                client=client,
                measurement="deribit_option",
                dataset_path=dataset_path,
                checkpoint=checkpoint,
                series_key_filter=None,
                series_tag_key_filter="instrument",
                series_tag_regex_filter=r"^ETH-[0-9]{1,2}SEP22-",
                start=None,
                end=None,
                limit=None,
                chunk_size=1000,
                rows_per_file=10,
                compression="zstd",
                compression_level=1,
            )

            self.assertEqual(result["status"], "exported")
            self.assertEqual(result["rows"], 4)
            self.assertEqual(result["parts"], 1)

            part_files = sorted(dataset_path.glob("part-*.parquet"))
            self.assertEqual([path.name for path in part_files], ["part-000000.parquet"])

            part_index = exporter.load_part_index(dataset_path, "deribit_option")
            self.assertEqual(len(part_index), 1)
            self.assertEqual(part_index[0]["rows"], 4)
            self.assertEqual(
                part_index[0]["tag_values"]["instrument"],
                ["ETH-16SEP22-1000-C", "ETH-9SEP22-1000-C"],
            )
            self.assertEqual(
                sorted(part_index[0]["series_keys"]),
                [
                    "deribit_option,instrument=ETH-16SEP22-1000-C",
                    "deribit_option,instrument=ETH-9SEP22-1000-C",
                ],
            )


if __name__ == "__main__":
    unittest.main()
