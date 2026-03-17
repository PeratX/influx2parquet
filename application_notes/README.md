# 导出结果应用说明

本目录面向两个主要场景：

- 人工快速理解 `/mnt/backup_hdd/exported` 和 `/mnt/backup_hdd/exported_parallel` 的内容。
- 给 LLM、代理脚本、RAG 预处理程序提供稳定、可引用的目录说明和示例代码。

## 根目录定位

- `/mnt/backup_hdd/exported`
  - 核心 keep-list 的最终 Parquet 数据集。
  - 结构稳定，优先用于分析、检索、特征提取和 LLM 下游任务。
- `/mnt/backup_hdd/exported_parallel`
  - 并行导出的补充数据。
  - 当前主要包括：
    - `binance_depth20`
    - `okex_depth`
  - 其中 `okex_depth` 目录还包含可恢复流水线状态和中间产物。

## 推荐使用顺序

1. 先看 [`exported.md`](./exported.md)。
2. 只有在需要深度盘口时，再看 [`exported_parallel.md`](./exported_parallel.md)。
3. 想给 LLM 一个紧凑、结构化的目录摘要时，优先运行 [`build_export_catalog.py`](./build_export_catalog.py)。

## 面向 LLM 的建议

- 不要先把整批 `part-*.parquet` 扫一遍。
- 优先读取每个数据集目录下的元数据文件：
  - `_summary.json`
  - `_description.md`
  - `_manifest.json`
  - `_pipeline_state_go.json`
  - `_tsm_index.json`
- 如果只是让 LLM 知道“有哪些数据、时间范围到哪里、列长什么样”，元数据通常已经够了。
- 只有在需要真实样本时，再抽几行 Parquet。

## 快速示例

生成一个适合贴给 LLM 的 JSON 目录摘要：

```bash
python3 /home/niko/influx2parquet/application_notes/build_export_catalog.py --pretty
```

只看稳定的核心导出：

```bash
python3 /home/niko/influx2parquet/application_notes/build_export_catalog.py \
  --exported-root /mnt/backup_hdd/exported \
  --parallel-root /mnt/backup_hdd/exported_parallel \
  --section exported \
  --pretty
```
