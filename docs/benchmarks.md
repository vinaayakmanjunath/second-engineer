# Benchmarks (generated)

_Generated 2026-11-10 18:50 UTC by `secondeng bench` on: Apple M1 Pro, 8 CPUs, 16 GB RAM, Darwin 27.2.0, Python 3.11.15._

## Storage

| layer | size | note |
|---|---:|---|
| raw Gloria CSV (3 files) | 811.1 MB | as published |
| landing (gzip JSON lines, satellite batches) | 177.9 MB | 5,163 files incl. re-sends |
| bronze (as received) | 176.5 MB | identical re-sends not stored |
| silver points (Parquet zstd, long format) | 209.4 MB | 80,739,179 readings |
| gold 1-minute rollup | 1,373,567 rows | in DuckDB |

Silver Parquet is **3.9x smaller** than the raw CSV.

## Full build vs one late batch

| step | full build (s) | one late batch (s) | re-run, nothing new (s) |
|---|---:|---:|---:|
| bronze | 5.39 | 0.06 | 0.03 |
| silver | 68.96 | 0.05 | 0.02 |
| gold | 5.04 | 3.17 | 2.88 |
| **total** | **79.4** | **3.3** | **2.9** |

- Full build: silver parsed 82,179,168 readings in 68.94 s (**1,192,039 readings/s**), 81 vessel-day partitions written.
- Peak memory of the full bronze + silver build: **4.6 GiB** (resident, one process; silver parses 400 files per chunk).
- Late batch `20260918T233110_GLORIA-A-20260918T2315.jsonl.gz`: silver rewrote **1 partition(s)** (GLORIA-A/2026-09-18); dbt rollup is incremental on the touched vessel-days.

## Idempotency (re-run with nothing new)

- bronze new files: 0; silver files processed: 0; partitions rewritten: 0
- silver partition files unchanged: **True**
- gold checksums identical: **True** (mart_rollup_1min, mart_alerts, mart_dq_daily)

## Query latency (median of 5)

One week of hourly `me.exh_temp_cyl3_c` for GLORIA-C:

- from silver raw Parquet (1 Hz): **11.6 ms**
- from the gold 1-minute rollup: **2.4 ms**
