# influx2parquet

把 InfluxDB `OptionData` 里的选定市场数据导出成紧凑、可恢复、易分析的 Parquet 数据集。

项目过程记录见：

- [blog.md](blog.md)

这个仓库现在同时包含三类东西：

- 通用的 Python 导出脚本，用于大部分 keep-list measurement。
- 面向 `okex_depth` 的 direct-TSM 导出流水线。
- 一些围绕恢复环境、检查、体积分析和目录说明的辅助脚本。

## 目标

默认需要保留的 measurement：

- `binance_depth`
- `binance_ticker`
- `binance_trade`
- `okex_index_ticker`
- `okex_ticker`
- `okex_trades`
- `candlestick`
- `perpetual`

可选的深度簿 measurement：

- `binance_depth20`
- `okex_depth`

其中：

- `binance_depth20` 是更深的 Binance 原始盘口流。
- `okex_depth` 是唯一的 OKX 原始盘口深度数据，不能被 `okex_ticker` / `okex_trades` 替代。

## 当前推荐路径

按今天这个仓库的实际状态，推荐这样理解：

- 大多数 measurement：优先用 [python/extract_optiondata_to_parquet.py](python/extract_optiondata_to_parquet.py)。
- 需要按 `instId` 并发拆任务的深度流：用 [python/export_parallel_instid.py](python/export_parallel_instid.py)。
- `okex_depth`：优先用 Go 版 direct TSM/WAL 导出器 [go/okex_depth_direct/cmd/okex_depth_direct/main.go](go/okex_depth_direct/cmd/okex_depth_direct/main.go)。
- Python 版 [python/export_okex_depth_tsm.py](python/export_okex_depth_tsm.py) 仍然保留，适合参考旧流水线、兼容旧中间产物，或在 Go 版不可用时兜底。

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
