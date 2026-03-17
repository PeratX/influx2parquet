# `/mnt/backup_hdd/exported` 说明

`/mnt/backup_hdd/exported` 存放的是核心 keep-list 的最终 Parquet 数据集。这里的数据通常已经是“可直接消费”的状态。

## 当前可见数据集

- `binance_depth.parquet`
- `binance_ticker.parquet`
- `binance_trade.parquet`
- `candlestick.parquet`
- `okex_index_ticker.parquet`
- `okex_ticker.parquet`
- `okex_trades.parquet`
- `perpetual.parquet`

## 每个数据集目录的典型结构

以 `binance_depth.parquet/` 为例：

```text
binance_depth.parquet/
  _checkpoint.json
  _description.md
  _summary.json
  part-000000.parquet
  part-000001.parquet
  ...
```

## 元数据文件含义

- `_summary.json`
  - 最重要。
  - 通常包含：
    - `measurement`
    - `dataset_path`
    - `dataset_size_bytes`
    - `record_count`
    - `part_count`
    - `date_range`
    - `table_format`
    - `compression`
- `_description.md`
  - 对字段语义、来源和查询模板的文字说明。
- `_checkpoint.json`
  - 导出脚本自己的完成标记与校验信息。

## 面向 LLM/代理的推荐读取策略

如果目标只是“理解这个数据集能不能用、列是什么、覆盖了什么时间范围”，建议顺序：

1. 先读 `_summary.json`
2. 再读 `_description.md`
3. 最后才按需抽样 `part-*.parquet`

这样能显著减少 token 和 I/O。

## 适合的使用场景

- 统一做市场数据分析
- 给 LLM 建立数据集目录索引
- 做 DuckDB / Polars / PyArrow 离线查询
- 做特征工程和回测前预清洗

## 不推荐的做法

- 直接把整个目录下所有 `part-*.parquet` 的 schema 一个个全扫出来
- 把 `_checkpoint.json` 当成唯一可信元数据来源
- 把 `exported_parallel` 中尚未收尾的目录和这里的最终数据集混为一谈

## DuckDB 示例

```sql
SELECT instId, time, bidPx, askPx
FROM read_parquet('/mnt/backup_hdd/exported/binance_depth.parquet/*.parquet')
WHERE instId = 'BTCUSDT'
ORDER BY time
LIMIT 20;
```

## 给 LLM 的简短提示词模板

```text
先阅读每个数据集目录下的 _summary.json，再决定是否需要读取具体 parquet part。
优先用 /mnt/backup_hdd/exported 下的数据；这里是最终、稳定、适合直接消费的导出结果。
```
