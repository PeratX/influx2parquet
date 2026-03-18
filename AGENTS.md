# Project Instructions

## Goal
Preserve market-data history from the InfluxDB `OptionData` database in durable, analyzable formats.

The immediate first priority is still:
- finish the offline `okex_depth` direct TSM/WAL pipeline
- keep `influxdb` offline until `okex_depth` `build` is done

After that, the broader mission is:
- bring `influxdb` back online
- preserve all market data, including spot, futures, options, and binary option / prediction-market related historical data
- continue exporting to Parquet where it is practical and useful for later analysis or model training

## First-Priority Measurements
These remain the first measurements to preserve and validate:
- `binance_depth`
- `binance_ticker`
- `binance_trade`
- `okex_index_ticker`
- `okex_ticker`
- `okex_trades`
- `candlestick`
- `perpetual`

Deeper order book datasets that remain important:
- `binance_depth20`
- `okex_depth`

## Exclusions
Always exclude clearly non-market or duplicate legacy items:
- `chainlink_eth_price`
- `deribit_perpetual`

Exclude because they are operational rather than market data:
- `settle_prices`
- `target_position`
- `hedge_target_position`

Exclude clearly test-only or typo / scratch measurements:
- `candlestick_chart_option_price_test`
- `candlstick`
- `option_market_metrics_test`
- `option_profit_daily_merge_test`
- `predict_product_price_delta_test`
- `predict_product_price_test`
- `test`
- `test_settle_price_table`

Do not exclude a measurement merely because it is option-related.
Option, binary option, prediction-market, and related derived market measurements are now in scope unless they are confirmed to be test-only or operational noise.

## Parquet Guidance
- Prefer one Parquet dataset per measurement.
- Preserve `time` and tag columns such as `instId`, `symbol`, `instrument`, and `exchange`.
- Prefer `perpetual` over `deribit_perpetual`; it is the same Deribit perpetual market stream with a more useful `exchange` tag.
- `okex_depth` is still the current first-priority export job.
- `binance_depth20` and `okex_depth` are larger and less convenient than reduced depth tables, but they remain unique raw order book sources and should not be dropped casually.
- For the expanded preservation mission, keep raw market-history coverage first and optimize compactness second.

## Depth Table Notes
- `binance_depth20` is the raw Binance depth stream. It stores multi-level book arrays in `a` and `b`, plus sequence fields such as `U`, `u`, and `pu`.
- `binance_depth` is a reduced top-of-book style view of the same Binance symbols. It keeps only `askPx`, `askSz`, `bidPx`, `bidSz`, and `E`.
- Because of that reduction, `binance_depth20` is only partially duplicated by `binance_depth`. Keep `binance_depth20` only if full order book reconstruction or deeper book context is needed.
- `okex_depth` is still unique in `OptionData`. It stores raw depth payloads in `asks` and `bids`, with `action`, `checksum`, and `ts`.
- `okex_ticker`, `okex_trades`, and `okex_index_ticker` do not replace `okex_depth`. They provide ticker, trade, and index-price data only.
- If space is tight, dropping `binance_depth20` after keeping `binance_depth` is the safer compromise. Dropping `okex_depth` loses unique order book data.

## Current `okex_depth` Pipeline
- Use [export_okex_depth_tsm.py](python/export_okex_depth_tsm.py) for `okex_depth`. It is the preferred direct-TSM pipeline and avoids the InfluxDB HTTP/JSON query path.
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
- [export_okex_depth_tsm.py](python/export_okex_depth_tsm.py) supports both directory layouts:
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
- Legacy exporters such as [extract_optiondata_to_parquet.py](python/extract_optiondata_to_parquet.py) and [export_parallel_instid.py](python/export_parallel_instid.py) still depend on `influxdb`.
- If `influxdb` is left running on the overlay-backed recovered database, it may generate large `.tsm` and `.tsm.tmp` files under `/opt/upper/data`, which fills the VM root filesystem.
- For this reason, direct `okex_depth` scan/export should prefer bindfs paths and can safely run with `influxdb` stopped.
- Because the broader mission now includes option and binary-option history, `influxdb` is expected to come back online after the offline `okex_depth` build finishes.

## Reset and Meta Sync
- Use [reset_optiondata_stack.sh](scripts/reset_optiondata_stack.sh) to tear down and rebuild the recovered OptionData mount stack.
- The reset script now force-unmounts overlay/bindfs paths, clears `/opt/upper/*` and `/opt/work/*`, rebuilds the recovery stack, and intentionally leaves `influxdb` stopped.
- After deleting `/opt/my_influx/meta/meta.db`, `optiondata-meta-sync.service` must be **restarted**, not merely started, because it is a `oneshot` unit with `RemainAfterExit=yes`.

## Current Export Snapshot
- As of 2026-03-16, the original core keep-list datasets are already exported to Parquet:
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
- After `okex_depth` is safely exported and built, the mission expands from the original keep-list to preserving the wider option / binary-option / prediction-market measurement set as well.
