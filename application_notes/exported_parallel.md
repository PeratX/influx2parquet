# `/mnt/backup_hdd/exported_parallel` 说明

`/mnt/backup_hdd/exported_parallel` 主要放两类内容：

- 并行分任务导出的最终数据集
- 仍在构建中的可恢复流水线状态和中间产物

当前顶层目录主要有：

- `binance_depth20`
- `okex_depth`

## `binance_depth20`

这是按 `instId` 分任务导出的最终 Parquet 数据集集合。

典型结构：

```text
binance_depth20/
  _manifest.json
  _logs/
  BTCUSDT.parquet/
  ETHUSDT.parquet/
  DOGEUSDT.parquet/
  1000SHIBUSDT.parquet/
```

### 关键文件

- `_manifest.json`
  - 汇总每个任务的：
    - `inst_id`
    - `dataset_path`
    - `status`
    - `log_path`
    - `series_key`
- `_logs/*.log`
  - 单任务日志

### 适合的使用方式

- 把它看成“一个 measurement 下的多个按 instId 切开的最终数据集”
- 先读 `_manifest.json`，再按目标 `inst_id` 定位具体 Parquet 目录

## `okex_depth`

这是最复杂的目录。它既包含最终目标，也包含导出流水线的状态和中间产物。

### 当前重要文件/目录

- `_tsm_index.json`
  - direct TSM `scan` 的结果
  - 记录命中的 `.tsm` 文件、`inst_ids`、大小、shard 等信息
- `_pipeline_state.json`
  - 旧 Python 版流水线状态
- `_pipeline_state_go.json`
  - 新 Go 版 direct TSM/WAL 流水线状态
- `_raw/`, `_merged/`, `_build/`
  - 旧 Python 版中间产物
- `_raw_go/`, `_merged_go/`, `_build_go/`
  - 新 Go 版中间产物
- `_raw.bak/`
  - 旧 raw 中间产物的备份，用于校验和对比

### 当前推荐路径

对于 `okex_depth`：

- 优先相信 Go 版流水线
- 优先看 `_pipeline_state_go.json`
- 优先使用 `_raw_go` / `_merged_go` / `_build_go`
- 旧 `_raw.bak` 主要用于验证，不应用作新的主消费路径

### 中间产物格式

旧 Python 版：

- `_raw/` / `_raw.bak/`
  - `*.tsv.zst`
  - 压缩文本

新 Go 版：

- `_raw_go/`
  - `*.bin.zst`
  - zstd 压缩的二进制 spool
- `_merged_go/`
  - 同样是 `*.bin.zst`

### 面向 LLM/脚本的建议

如果你不是在调试导出器本身：

- 不要优先消费 `_raw*` 和 `_merged*`
- 只把这些目录当成“流水线内部层”
- 真正适合给下游分析和 LLM 使用的，仍然应该是最终 Parquet 数据集

如果你是在做导出质量验证：

- 对比 `_raw.bak` 和 `_raw_go`
- 优先比较：
  - `field_record_counts`
  - `min_time_ns`
  - `max_time_ns`
- 必要时再做逐条记录比对

本仓库里已经有现成脚本：

- `/home/niko/influx2parquet/python/compare_okex_depth_raw.py`

### 当前最重要的现实区别

- `binance_depth20`：基本是“已完成的并行最终结果”
- `okex_depth`：是“正在构建/可恢复/带中间层”的工作目录

所以对于代理或 LLM：

- 看到 `exported_parallel/binance_depth20` 可以较放心地当最终数据集集合用
- 看到 `exported_parallel/okex_depth` 必须先判断当前阶段和目录状态，不能默认它已经是最终成品
