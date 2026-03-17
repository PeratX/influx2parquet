# Project Instructions

## Goal
Extract selected market-data measurements from the InfluxDB `OptionData` database into Parquet files that are compact and easy to use.

## Keep List
Default measurements to export:
- `binance_depth`
- `binance_ticker`
- `binance_trade`
- `okex_index_ticker`
- `okex_ticker`
- `okex_trades`
- `candlestick`
- `perpetual`

Optional measurements only if deeper order book snapshots are explicitly needed:
- `binance_depth20`
- `okex_depth`

## Exclusions
Always exclude:
- `chainlink_eth_price`
- `deribit_perpetual`

Exclude because they are not market data:
- `settle_prices`
- `target_position`
- `hedge_target_position`

Exclude because they are option-related or test data:
- `candlestick_chart`
- `candlestick_chart_option_price`
- `candlestick_chart_option_price_test`
- `candlstick`
- `deribit_delta_recorder`
- `deribit_option`
- `dex_option_price`
- `dex_option_price_model`
- `implied_voltality`
- `option`
- `option_market_metrics_online`
- `option_market_metrics_symbol_profit_online`
- `option_market_metrics_test`
- `option_market_metrics_total_profit_online`
- `option_price`
- `option_profit_daily_merge_test`
- `option_settle_prices`
- `options`
- `predict_product_price`
- `predict_product_price_delta_test`
- `predict_product_price_test`
- `test`
- `test_settle_price_table`

## Parquet Guidance
- Prefer one Parquet dataset per measurement.
- Preserve `time` and tag columns such as `instId`, `symbol`, `instrument`, and `exchange`.
- Prefer `perpetual` over `deribit_perpetual`; it is the same Deribit perpetual market stream with a more useful `exchange` tag.
- Treat `binance_depth20` and `okex_depth` as optional because they are less compact and less convenient to work with than the default keep list.

## Depth Table Notes
- `binance_depth20` is the raw Binance depth stream. It stores multi-level book arrays in `a` and `b`, plus sequence fields such as `U`, `u`, and `pu`.
- `binance_depth` is a reduced top-of-book style view of the same Binance symbols. It keeps only `askPx`, `askSz`, `bidPx`, `bidSz`, and `E`.
- Because of that reduction, `binance_depth20` is only partially duplicated by `binance_depth`. Keep `binance_depth20` only if full order book reconstruction or deeper book context is needed.
- `okex_depth` is still unique in `OptionData`. It stores raw depth payloads in `asks` and `bids`, with `action`, `checksum`, and `ts`.
- `okex_ticker`, `okex_trades`, and `okex_index_ticker` do not replace `okex_depth`. They provide ticker, trade, and index-price data only.
- If space is tight, dropping `binance_depth20` after keeping `binance_depth` is the safer compromise. Dropping `okex_depth` loses unique order book data.

## Current `okex_depth` Pipeline
- Use [export_okex_depth_tsm.py](/home/niko/influx2parquet/export_okex_depth_tsm.py) for `okex_depth`. It is the preferred direct-TSM pipeline and avoids the InfluxDB HTTP/JSON query path.
- The pipeline has 4 resumable phases: `scan`, `export`, `merge`, `build`.
- Pipeline state lives under `<output-dir>/okex_depth/`:
  - `_pipeline_state.json`
  - `_tsm_index.json`
  - `_raw/`
  - `_merged/`
  - final datasets `<instId>.parquet/`
- Raw and merged intermediate spool files are compressed as `.tsv.zst`. The script remains backward-compatible with older plain `.tsv` intermediates.
- `scan`, `export`, `merge`, and `build` all have progress output and resumable state.

## Influx Tree Layouts
- [export_okex_depth_tsm.py](/home/niko/influx2parquet/export_okex_depth_tsm.py) supports both directory layouts:
  - `data-root`: `<data-dir>/<database>/<retention>`
  - `database-root`: `<database-root>/<retention>`
- Typical paths in this recovery environment:
  - overlay data-root: `/opt/my_influx/data`
  - overlay wal-root: `/opt/my_influx/wal`
  - bindfs database-root for data: `/mnt/mapped/data_target`
  - bindfs database-root for wal: `/mnt/mapped/wal_target`
- Prefer scanning/exporting directly from bindfs database-root when possible, especially for `okex_depth`, to avoid overlay pollution.

## Recovery Stack Caveats
- The direct TSM pipeline does **not** require `influxdb` to be running.
- Legacy exporters such as [extract_optiondata_to_parquet.py](/home/niko/influx2parquet/extract_optiondata_to_parquet.py) and [export_parallel_instid.py](/home/niko/influx2parquet/export_parallel_instid.py) still depend on `influxdb`.
- If `influxdb` is left running on the overlay-backed recovered database, it may generate large `.tsm` and `.tsm.tmp` files under `/opt/upper/data`, which fills the VM root filesystem.
- For this reason, direct `okex_depth` scan/export should prefer bindfs paths and can safely run with `influxdb` stopped.

## Reset and Meta Sync
- Use [reset_optiondata_stack.sh](/home/niko/influx2parquet/scripts/reset_optiondata_stack.sh) to tear down and rebuild the recovered OptionData mount stack.
- The reset script now force-unmounts overlay/bindfs paths, clears `/opt/upper/*` and `/opt/work/*`, rebuilds the recovery stack, and intentionally leaves `influxdb` stopped.
- After deleting `/opt/my_influx/meta/meta.db`, `optiondata-meta-sync.service` must be **restarted**, not merely started, because it is a `oneshot` unit with `RemainAfterExit=yes`.

## Current Export Snapshot
- As of 2026-03-16, the core keep-list datasets are already exported to Parquet:
  - `binance_depth`
  - `binance_ticker`
  - `binance_trade`
  - `okex_index_ticker`
  - `okex_ticker`
  - `okex_trades`
  - `candlestick`
  - `perpetual`
- `binance_depth20` is mostly exported; one dataset was still active during this session.
- The remaining difficult legacy item is `okex_depth`, which should continue to use the direct TSM pipeline rather than the legacy HTTP exporter.
