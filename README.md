# influx2parquet

把 InfluxDB `OptionData` 里的市场数据尽可能完整地保留下来，并在合适的地方导出成紧凑、可恢复、易分析的 Parquet 数据集。

项目过程记录见：

- [blog.md](blog.md)

这个仓库现在同时包含三类东西：

- 通用的 Python 导出脚本，用于大部分 keep-list measurement。
- 面向 `okex_depth` 的 direct-TSM 导出流水线。
- 一些围绕恢复环境、检查、体积分析和目录说明的辅助脚本。

## 目标

当前目标分两层：

- 第一优先级仍然是离线完成 `okex_depth` 的 direct TSM/WAL `scan -> export -> merge -> build`。
- 在 `okex_depth` 完成前，尽量保持 `influxdb` 离线，避免恢复环境继续生成 overlay 污染。
- `okex_depth` 完成后，重新上线 `influxdb`，继续保全更完整的市场数据历史。

原始第一优先级 measurement：

- `binance_depth`
- `binance_ticker`
- `binance_trade`
- `okex_index_ticker`
- `okex_ticker`
- `okex_trades`
- `candlestick`
- `perpetual`

高价值的深度簿 measurement：

- `binance_depth20`
- `okex_depth`

其中：

- `binance_depth20` 是更深的 Binance 原始盘口流。
- `okex_depth` 是唯一的 OKX 原始盘口深度数据，不能被 `okex_ticker` / `okex_trades` 替代。

新的业务上下文下，保全范围已经扩大：

- 不再只局限于现货 / 永续 / ticker / trade。
- option、binary option、prediction-market 相关历史数据现在也属于保全目标。
- 因此，以前仅因“option 相关”而被默认排除的 measurement，需要重新评估，而不是直接丢弃。

## 当前推荐路径

按今天这个仓库的实际状态，推荐这样理解：

- 大多数 measurement：优先用 [python/extract_optiondata_to_parquet.py](python/extract_optiondata_to_parquet.py)。
- 需要按 `instId` 并发拆任务的深度流：用 [python/export_parallel_instid.py](python/export_parallel_instid.py)。
- `okex_depth`：优先用 Go 版 direct TSM/WAL 导出器 [go/okex_depth_direct/cmd/okex_depth_direct/main.go](go/okex_depth_direct/cmd/okex_depth_direct/main.go)。
- Python 版 [python/export_okex_depth_tsm.py](python/export_okex_depth_tsm.py) 仍然保留，适合参考旧流水线、兼容旧中间产物，或在 Go 版不可用时兜底。

当前顺序不是“所有 measurement 一起并行推进”，而是：

1. 先完成 `okex_depth`
2. 再让 `influxdb` 回到在线状态
3. 再继续处理更广泛的 option / binary option / prediction-market 数据

## 仓库结构

- [python](python)
  Python 脚本主目录。
- [go/okex_depth_direct](go/okex_depth_direct)
  `okex_depth` 的高吞吐 direct TSM/WAL Go 导出器。
- [scripts](scripts)
  恢复环境的挂载/重置脚本。
- [application_notes](application_notes)
  面向 LLM/人工阅读的目录说明和输出目录摘要工具。
- [tests](tests)
  Python 侧回归测试。
- [AGENTS.md](AGENTS.md)
  项目约束、keep-list、恢复环境说明。
- [MEMORY.md](MEMORY.md)
  当前协作上下文备忘。

## 导出流程总览

### 通用 measurement

典型流程是：

1. 通过 Influx HTTP 查询按 measurement 导出。
2. 在本地按行缓冲并写成 Parquet dataset。
3. 使用 `_checkpoint.json`、`summary.json` 等元数据支持断点续跑。

对应脚本：

- [python/extract_optiondata_to_parquet.py](python/extract_optiondata_to_parquet.py)

这个通用 HTTP 导出器仍然适合后续更广泛的 measurement 保全工作，包括重新纳入的 option 类市场数据。

### `okex_depth`

`okex_depth` 的数据量和 payload 形态都更特殊，因此单独走 direct TSM/WAL 流水线。

Go 版流水线分 4 个阶段：

1. `scan`
   扫描 TSM，继承或生成 `_tsm_index.json`。
2. `export`
   直接读 TSM/WAL，写 `_raw_go/*.bin.zst`。
3. `merge`
   把同一 `instId/field` 的多个 source 合并去重，写 `_merged_go/*.bin.zst`。
4. `build`
   从 merged 字段流重建行，写最终 `<instId>.parquet/`。

中间目录语义：

- `_raw_go`
  export 阶段原始中间层。
- `_merged_go`
  merge 阶段去重后中间层。
- `_build_go`
  build 阶段临时施工目录。
- `<instId>.parquet/`
  最终数据集。

在当前项目阶段，`okex_depth` 仍然是整个仓库的第一优先级任务。

### `okex_depth` Final Parquet Schema

最终 `<instId>.parquet/` 里的核心列现在是：

- `time`
  Parquet `timestamp(nanosecond:utc)`，对应记录时间。
- `action`
  `string`，原始值如 `snapshot` / `update`。
- `asks`
  `string`，直接保留原始盘口 JSON 文本，例如 `[[\"65000.1\",\"12.5\",\"0\",\"1\"], ...]`。
- `bids`
  `string`，直接保留原始盘口 JSON 文本。
- `checksum`
  `int64`
- `ts`
  `string`，保留原始字符串时间戳。

当前格式回到直接 JSON 文本存储：

- `instId` 不再按行重复写入；每个 dataset 目录本身就对应一个 `instId`
- 不依赖 parquet metadata 里的额外 scale 信息
- 不再使用 packed binary 或结构化盘口编码
- `asks` / `bids` 可以直接被 DuckDB、PyArrow、Polars 或自定义脚本按 JSON 文本解析
- 文件体积会更大，但写入路径更简单，也更接近最初的高吞吐版本

如果你需要在 Python 里读取并把结构化盘口重建回可读数组，可以直接参考：

- [python/read_okex_depth_compact_parquet.py](python/read_okex_depth_compact_parquet.py)

这个读取脚本只面向当前这版 schema：

- `instId` 不在每行列里；默认从 dataset 目录名理解
- `asks` / `bids` 是原始 JSON 文本
- `--decode-book` 会把盘口文本解析成数组

示例：

```bash
/home/niko/influx2parquet/.venv/bin/python \
  /home/niko/influx2parquet/python/read_okex_depth_compact_parquet.py \
  /mnt/intelssd/okex_depth/BTC-USDT-SWAP.parquet \
  --limit 3 \
  --decode-book
```

## 主要脚本说明

### Python

- [python/extract_optiondata_to_parquet.py](python/extract_optiondata_to_parquet.py)
  通用单 measurement 导出器。适合默认 keep-list 里的大多数 measurement。依赖 InfluxDB HTTP 接口。

- [python/export_parallel_instid.py](python/export_parallel_instid.py)
  把 `okex_depth` 或 `binance_depth20` 按 `instId` 拆成多个 worker 并发跑。仍然基于 Python HTTP 导出器，不是 direct TSM。

- [python/export_okex_depth_tsm.py](python/export_okex_depth_tsm.py)
  旧版 direct-TSM `okex_depth` 流水线。支持 `scan/export/merge/build` 四阶段，原始和合并中间层是 `.tsv.zst`。

- [python/compare_okex_depth_raw.py](python/compare_okex_depth_raw.py)
  对比旧 Python `_raw.bak` 和新 Go `_raw_go` 中间产物。可做 manifest 级比较，也可做逐条记录比对。

- [python/check_okex_depth_merge_before_raw_cleanup.py](python/check_okex_depth_merge_before_raw_cleanup.py)
  在 `merge` 完成后检查 `_merged_go` 是否足够可靠，帮助判断 `_raw_go` 是否可以手动删除。只检查，不删除。

- [python/read_okex_depth_compact_parquet.py](python/read_okex_depth_compact_parquet.py)
  读取当前 `okex_depth` Parquet 格式，并可把 `asks` / `bids` 的原始 JSON 文本解析成数组。

- [python/compare_parquet_sizes.py](python/compare_parquet_sizes.py)
  统计导出后 Parquet 的真实落盘体积、Parquet 元数据中的压缩前大小、节省空间和压缩比。

- [python/recalculate_dataset_checkpoint.py](python/recalculate_dataset_checkpoint.py)
  某个 dataset 的 part 文件丢失后，重建 `_checkpoint.json` 和相关元数据。

### Go

- [go/okex_depth_direct/cmd/okex_depth_direct/main.go](go/okex_depth_direct/cmd/okex_depth_direct/main.go)
  当前最快的 `okex_depth` direct TSM/WAL 导出器。绕过 `influx_inspect export` 的 line protocol 文本路径，直接读取 TSM/WAL，写二进制 spool，再构建 Parquet。

- [go/okex_depth_direct/internal/influxlite/tsm1](go/okex_depth_direct/internal/influxlite/tsm1)
  从官方 InfluxDB 读取逻辑裁下来的轻量 TSM/WAL 读取层，用来避免把整套 Influx/Flux 依赖链编进来。

### Shell / 文档

- [scripts/reset_optiondata_stack.sh](scripts/reset_optiondata_stack.sh)
  重新搭建恢复环境的 overlay/bindfs 栈，并清掉污染的 upper/work 目录。默认保留 `influxdb` 停止状态。

- [application_notes/README.md](application_notes/README.md)
  输出目录说明入口。

- [application_notes/exported.md](application_notes/exported.md)
  `/mnt/backup_hdd/exported` 的用途说明。

- [application_notes/exported_parallel.md](application_notes/exported_parallel.md)
  `/mnt/backup_hdd/exported_parallel` 的用途说明，尤其是 `okex_depth` 流水线目录结构。

- [application_notes/build_export_catalog.py](application_notes/build_export_catalog.py)
  为 LLM 或人工生成输出目录摘要 JSON。

## 输出目录

常见输出根目录：

- `/mnt/backup_hdd/exported`
  主要放已经完成的通用 measurement Parquet。

- `/mnt/backup_hdd/exported_parallel`
  主要放并发导出和 `okex_depth` 流水线产物。

`okex_depth` 常见状态文件：

- `_pipeline_state_go.json`
  Go 流水线状态。
- `_pipeline_state.json`
  Python 旧流水线状态。
- `_tsm_index.json`
  scan 阶段的匹配结果。

## 常用命令

### 1. 运行通用 Python 导出器

```bash
/home/niko/influx2parquet/.venv/bin/python \
  /home/niko/influx2parquet/python/extract_optiondata_to_parquet.py \
  binance_trade \
  --host 127.0.0.1 \
  --port 8086 \
  --output-dir /mnt/backup_hdd/exported
```

### 2. 构建 Go 版 `okex_depth` 导出器

```bash
cd /home/niko/influx2parquet/go/okex_depth_direct
go build -o /tmp/okex_depth_direct ./cmd/okex_depth_direct
```

### 3. 运行 Go 版 `okex_depth` 导出器

```bash
sudo -u influxdb /tmp/okex_depth_direct \
  --data-dir /mnt/mapped/data_target \
  --wal-dir /mnt/mapped/wal_target \
  --output-dir /mnt/backup_hdd/exported_parallel \
  --workers 8 \
  --build-workers 2
```

### 4. 为 `okex_depth` 拆分工作盘

Go 版支持把元数据、raw、merged/build、final 分开：

```bash
sudo -u influxdb /tmp/okex_depth_direct \
  --data-dir /mnt/mapped/data_target \
  --wal-dir /mnt/mapped/wal_target \
  --output-dir /mnt/meta \
  --raw-dir /mnt/hdd1 \
  --merged-dir /mnt/ssd2 \
  --build-dir /mnt/ssd2 \
  --final-dir /mnt/backup_hdd/exported_parallel
```

语义是：

- `--output-dir`
  metadata 根目录。
- `--raw-dir`
  export 中间层所在盘。
- `--merged-dir`
  merge 中间层所在盘。
- `--build-dir`
  build 临时目录所在盘。
- `--final-dir`
  最终 Parquet 所在盘。

## 运行依赖

### Python

主要依赖：

- `pyarrow`
- `requests`
- `zstandard`

建议统一用仓库内虚拟环境：

```bash
/home/niko/influx2parquet/.venv/bin/python ...
```

### Go

Go 版 `okex_depth` 导出器使用独立模块：

- [go/okex_depth_direct/go.mod](go/okex_depth_direct/go.mod)

常见构建命令：

```bash
cd /home/niko/influx2parquet/go/okex_depth_direct
go test ./...
go build ./cmd/okex_depth_direct
```

## 恢复环境注意事项

- direct TSM 路线不要求 `influxdb` 运行。
- Python 通用导出器和并发 `instId` 导出器仍然依赖 `influxdb` HTTP 接口。
- 恢复环境里优先使用 bindfs 的 database-root：
  - `/mnt/mapped/data_target`
  - `/mnt/mapped/wal_target`
- 不要长期让 overlay 上的 `influxdb` 挂着跑，否则容易在 `/opt/upper/data` 生成大量 `.tsm`/`.tmp` 污染物。

## 这个仓库现在最值得先看哪几个文件

第一次接手时，建议先看：

1. [AGENTS.md](AGENTS.md)
2. [README.md](README.md)
3. [python/extract_optiondata_to_parquet.py](python/extract_optiondata_to_parquet.py)
4. [go/okex_depth_direct/cmd/okex_depth_direct/main.go](go/okex_depth_direct/cmd/okex_depth_direct/main.go)
5. [application_notes/exported_parallel.md](application_notes/exported_parallel.md)

如果你只关心 `okex_depth`：

1. [go/okex_depth_direct/cmd/okex_depth_direct/main.go](go/okex_depth_direct/cmd/okex_depth_direct/main.go)
2. [python/check_okex_depth_merge_before_raw_cleanup.py](python/check_okex_depth_merge_before_raw_cleanup.py)
3. [python/compare_okex_depth_raw.py](python/compare_okex_depth_raw.py)
