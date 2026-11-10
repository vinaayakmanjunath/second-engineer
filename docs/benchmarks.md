# Benchmarks (generated)

_Generated 2026-11-10 16:03 UTC by `secondeng bench` on: Apple M1 Pro, 8 CPUs, 16 GB RAM, Darwin 27.2.0, Python 3.11.15._

## Storage

| layer | size | note |
|---|---:|---|
| raw Gloria CSV (3 files) | 811.1 MB | as published |
| landing (gzip JSON lines, satellite batches) | 178.3 MB | 5,174 files incl. re-sends |
| bronze (as received) | 176.7 MB | identical re-sends not stored |
| silver points (Parquet zstd, long format) | 208.9 MB | 80,740,175 readings |
| gold 1-minute rollup | 1,373,567 rows | in DuckDB |

Silver Parquet is **3.9x smaller** than the raw CSV.

## Full build vs one late batch

| step | full build (s) | one late batch (s) | re-run, nothing new (s) |
|---|---:|---:|---:|
| bronze | 5.53 | 0.04 | 0.02 |
| silver | 60.42 | 0.06 | 0.02 |
| gold | 4.24 | 4.69 | 3.68 |
| **total** | **70.2** | **4.8** | **3.7** |

- Full build: silver parsed 82,179,168 readings in 60.4 s (**1,360,582 readings/s**), 81 vessel-day partitions written.
- Late batch `20260918T233128_GLORIA-A-20260918T2315.jsonl.gz`: silver rewrote **1 partition(s)** (GLORIA-A/2026-09-18); dbt rollup is incremental on the touched vessel-days.

## Idempotency (re-run with nothing new)

- bronze new files: 0; silver files processed: 0; partitions rewritten: 0
- silver partition files unchanged: **True**
- gold checksums identical: **True** (mart_rollup_1min, mart_alerts, mart_dq_daily)

## Query latency (median of 5)

One week of hourly `me.exh_temp_cyl3_c` for GLORIA-C:

- from silver raw Parquet (1 Hz): **13.0 ms**
- from the gold 1-minute rollup: **2.7 ms**
