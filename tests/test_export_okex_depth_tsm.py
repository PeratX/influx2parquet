from __future__ import annotations

import io
from argparse import Namespace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import export_okex_depth_tsm as exporter


class FakeProcess:
    def __init__(self, stdout_text: str) -> None:
        self.stdout = io.StringIO(stdout_text)

    def poll(self) -> int | None:
        return 0

    def terminate(self) -> None:
        return None

    def kill(self) -> None:
        return None

    def wait(self) -> int:
        return 0


class ExportSingleTsmTests(unittest.TestCase):
    def test_reuses_spool_handle_for_same_instid_and_field(self) -> None:
        raw_output = (
            'writing out tsm file data for OptionData/autogen...okex_depth,instId=BTC-USDT-SWAP asks="[[1,2]]" 100\n'
            'okex_depth,instId=BTC-USDT-SWAP asks="[[3,4]]" 200\n'
        )
        args = Namespace(
            inspect_binary="influx_inspect",
            database="OptionData",
            retention="autogen",
            start="2022-05-26T08:50:23.185000000Z",
            end="2023-11-21T04:31:03.229056001Z",
            export_queue_bytes=1,
            progress_interval=3600,
        )
        source_file = exporter.TsmFile(
            shard_id=1,
            generation=1,
            sequence=1,
            rel_path="1/000000001-000000001.tsm",
            abs_path="/tmp/fake.tsm",
            size_bytes=123,
        )
        data_tree = exporter.InfluxTree(
            role="data",
            input_root="/tmp/data",
            database_root="/tmp/data/OptionData",
            retention_root="/tmp/data/OptionData/autogen",
            layout="data-root",
        )
        wal_tree = exporter.InfluxTree(
            role="wal",
            input_root="/tmp/wal",
            database_root="/tmp/wal/OptionData",
            retention_root="/tmp/wal/OptionData/autogen",
            layout="data-root",
        )

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with patch("export_okex_depth_tsm.subprocess.Popen", return_value=FakeProcess(raw_output)):
                result = exporter.export_single_tsm(
                    args=args,
                    source_file=source_file,
                    allowed_inst_ids={"BTC-USDT-SWAP"},
                    root=root,
                    data_tree=data_tree,
                    wal_tree=wal_tree,
                )

            self.assertEqual(result.matched_lines, 2)
            asks_path = exporter.compressed_field_path(
                exporter.raw_root(root) / source_file.file_id / "BTC-USDT-SWAP",
                "asks",
            )
            self.assertTrue(asks_path.exists())
            rows = list(exporter.tsv_iter(asks_path))
            self.assertEqual(rows, [(100, "[[1,2]]"), (200, "[[3,4]]")])

    def test_ignores_arbitrary_prefix_before_measurement(self) -> None:
        parsed = exporter.parse_okex_depth_lp_line(
            'noise-prefix okex_depth,instId=BTC-USDT-SWAP asks="[[\\"1\\", \\"2\\"]]" 123\r\n'
        )
        self.assertEqual(parsed, ("BTC-USDT-SWAP", {"asks": '[["1", "2"]]'}, 123))

    def test_parses_multiple_tags_and_multiple_fields(self) -> None:
        parsed = exporter.parse_okex_depth_lp_line(
            'okex_depth,channel=books,instId=BTC-USDT-SWAP asks="[[\\"1\\", \\"2\\"]]",checksum=42i 123'
        )
        self.assertEqual(
            parsed,
            ("BTC-USDT-SWAP", {"asks": '[["1", "2"]]', "checksum": 42}, 123),
        )

    def test_bounded_queue_accepts_oversized_line_when_empty(self) -> None:
        queue = exporter.BoundedTextQueue(1)
        self.assertTrue(queue.put("okex_depth,instId=BTC-USDT-SWAP action=\"update\" 1\n"))
        queued = queue.get()
        self.assertIsNotNone(queued)
        assert queued is not None
        self.assertIn("okex_depth,instId=BTC-USDT-SWAP", queued[0])
        queue.close()
        self.assertIsNone(queue.get())


if __name__ == "__main__":
    unittest.main()
