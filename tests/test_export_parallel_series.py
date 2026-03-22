from __future__ import annotations

import sys
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))

import export_parallel_series as exporter


class FakeInfluxClient:
    def show_series_keys(self, measurement: str) -> list[str]:
        assert measurement == "option_price"
        return [
            "option_price,expiration=1Apr23,instId=BTC-1APR23-27500-CALL,side=c,underlying=BTC",
            "option_price,expiration=7Apr23,instId=BTC-7APR23-28000-PUT,side=p,underlying=BTC",
            "option_price,expiration=5May23,instId=BTC-5MAY23-29000-CALL,side=c,underlying=BTC",
        ]


class ExportParallelSeriesTests(unittest.TestCase):
    def test_contract_month_bucket_supports_option_price_instid(self) -> None:
        bucket, regex = exporter.contract_month_bucket("BTC-1APR23-27500-CALL")
        self.assertEqual(bucket, "BTC-APR23")
        self.assertEqual(regex, r"^BTC-[0-9]{1,2}APR23-")

    def test_discover_base_tasks_groups_option_price_by_instid_month(self) -> None:
        args = Namespace(
            host="127.0.0.1",
            port=8086,
            database="OptionData",
            username=None,
            password=None,
            measurement="option_price",
            shard_values=None,
            shard_tag=None,
            group_by="instrument-month",
        )

        with patch.object(exporter.exporter, "InfluxClient", return_value=FakeInfluxClient()):
            shard_tag, tasks = exporter.discover_base_tasks(args)

        self.assertEqual(shard_tag, "instrument_month")
        self.assertEqual([task.shard_value for task in tasks], ["BTC-APR23", "BTC-MAY23"])
        self.assertTrue(all(task.series_tag_key == "instId" for task in tasks))
        self.assertEqual(tasks[0].series_tag_regex, r"^BTC-[0-9]{1,2}APR23-")
        self.assertEqual(tasks[1].series_tag_regex, r"^BTC-[0-9]{1,2}MAY23-")


if __name__ == "__main__":
    unittest.main()
