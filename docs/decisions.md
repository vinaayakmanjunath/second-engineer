# Design decisions

Short records of the choices that matter, with the trade-off. All measured numbers live in the
generated docs (`results.md`, `benchmarks.md`, `sensitivity.md`, `data_profile.md`).

## Storage and engine: Parquet + DuckDB, not a database server
Columnar files plus an in-process engine: no ops, fast scans of 1 Hz data, and the same SQL runs in
CI. Every path goes through `lake_root` in `config/settings.yaml`, and the layer layout (bronze
files, hive-partitioned silver, gold tables) carries over unchanged to ADLS Gen2 or S3.
**Trade-off:** no concurrent writers and no transactions across files. At fleet scale, use Delta or
Iceberg on ADLS for the lake (MERGE, time travel, compaction), or a time-series store (Azure Data
Explorer, or MongoDB time-series collections) for hot queries.

## Silver is long/narrow: one row per (vessel, sensor, timestamp)
Loggers differ in which channels they have, so a long table needs no schema migration when a
vessel or channel is added, and every reading keeps its batch lineage.
**Trade-off:** more rows, and pivots cost compute. With a stable sensor set per system, wide
per-system tables are cheaper to scan.

## Partition silver by vessel and UTC date; rewrite whole partitions
A late batch touches one or two vessel-days. Silver reads each affected partition with the new rows,
deduplicates on (sensor, timestamp) keeping the latest ingest, writes a temp file and renames it
over the old one. Readers never see a half-written file, and untouched partitions are never
rewritten. Hourly partitions would create too many small files at 1 Hz × 20 sensors.
**Trade-off:** rewriting a partition costs O(partition). Delta or Iceberg MERGE would replace this.

## Idempotency at two levels
Bronze skips a file whose sha256 it already has (identical re-send under a new name). Silver skips
a `batch_id` already loaded from another file (re-send with a new envelope). Row-level dedupe on
(vessel, sensor, ts) is the final guard. Files are marked processed only after their partitions
are written, so a crash re-processes them safely.

## Vessel sensor assignment is configuration, not code
`config/sensors.yaml` has one canonical registry and, per logger, a map of raw tag → canonical
sensor with scale and offset, plus the logger's clock timezone. Onboarding a vessel with a new
logger means adding a mapping block. Unmapped tags are quarantined and counted, never silently
dropped. GLORIA-B uses a simulated second logger (vendor tags, psi and °F, UTC clock) to prove
that the mapping round-trips (`tests/test_mapping.py`).

## Quarantine instead of drop
Rows go to `silver/quarantine` with one reason each, aggregated per file, vessel, hour, tag and
reason: unknown logger, unparseable or blank timestamp, a local time that does not exist or occurs
twice (DST change), a timestamp in the future or more than 30 days before it was sent (a logger
clock reset), an unknown tag, a missing value, or an out-of-range value. `evaluate` reconciles
valid + quarantined readings against readings sent, per vessel.
A whole file that cannot be read (cut off in transit, not gzip) is kept under `bronze/_rejected/`
as `corrupt_file`; one that cannot be parsed is logged `unparseable`. Neither blocks other files.

## Crash safety
Silver writes partitions, then the batch log and quarantine parts, and marks files done in the
bronze manifest last. A crash in between re-processes the same files: points are deduped on
(sensor, ts), and the logs are read as each file's latest record (`silver.read_log`,
`stg_quarantine`, `stg_batches`), so nothing is counted twice. Two versions of one reading: the
one the ship sent last wins, whatever order they arrived in.

## Normal model: a straight line against engine speed, plus a robust sigma
Exhaust temperature on this engine follows rpm closely, so fixed thresholds or coarse load bands
either miss faults or cry wolf. Per vessel and sensor, `value = a + b·rpm` is fitted on the hours
the engine was running in the 10 calendar days after it first ran (46 running hours for the
GLORIA-C main engine), with sigma = 1.4826 × MAD of the residuals, floored at 1% of the median. The floor
binds often (quantised or very steady sensors; `mart_baselines.sigma_source` says which); a
per-sensor floor based on instrument resolution would be better than a percentage of °C. Constant-speed generators get a flat median baseline. An engine running at a
speed outside the learned range is not scored, so the model never extrapolates. Seawater outlet
temperature is not scored because it mostly follows the sea, not the engine.
**Why not ML:** with one ship and no labelled failures, a model I can explain line by line is worth
more than one I can't validate.
**Known weakness:** this ship's exhaust temperatures also drift with conditions the data does not
contain (weather, draft, fouling). The healthy vessels still get false alerts (`results.md`), and
the next steps would be more context features or a slowly adapting baseline.

## Alert rule
At least 3 sensors of one system at |z| ≥ 3 in the same running hour, for at least 3 such hours;
flagged hours more than 3 h apart start a new alert. Requiring several sensors and persistence
trades a little delay for far fewer single-sensor blips. Thresholds were picked once, from a small
grid, mainly on false alerts on the two healthy vessels. The fault severity was then varied
separately (`sensitivity.md`) rather than tuned.

## Fault injection is a documented assumption, and every claim is checked against a control
Fault direction comes from the bench: mean relative change per measurement between the
pre-anomaly and anomaly segments. The bench faults are mild, so the main run injects 3× bench
severity as the deviation at failure. At 1× the fault is already visible hour by hour against the
control (`sensitivity.md`), but the likely-cause ranking only comes out right from 3×. `secondeng evaluate` always re-runs the same vessel with no
fault (the control) and reports both side by side. On this ship the control also raises alerts
inside the ramp window, because exhaust temperatures drift naturally. So no lead time is claimed.
What the fault measurably changes is how many hours are flagged, how high z peaks, and whether
the likely cause comes out right. `sensitivity.md` repeats this at 0×, 1×, 2×, 3× and 5×.
A lead time would count only if the control stayed quiet in the same window.

## Fault match: cosine similarity on three measurements
Exhaust, jacket-water and lube-oil temperature are all the bench and the fleet share. Air-cooler
fouling and turbine degradation point in almost the same direction there, so the ranking between
them is weak. It is shown as "likely cause", with its similarity score, not as a diagnosis.

## Orchestration: a CLI plus GitHub Actions
`secondeng run` runs the steps in order; each step is idempotent and logs to `lake/_runs.jsonl`.
In Airflow this is one DAG: `simulate >> ingest >> silver >> dbt_build >> evaluate >> export`,
where ingest and silver are sensors on new landing files. dbt owns gold, tests and lineage.

## Dashboard: static files on GitHub Pages
Gold is exported to small JSON files (about 1 MB in total) and drawn with Observable Plot, pinned on
jsDelivr. It is always on, free and has no server to wake up. I chose JSON over DuckDB-WASM because
loading the WASM engine is a ~30 MB cold start, which is a real risk during a live screen share.
The API serves the same gold tables from Postgres (below).

## Serving store: Postgres for the API, DuckDB for the batch
DuckDB is a fast in-process engine with one writer: rebuilding gold locks the file, so an API reading
it directly would go down on every run, and it has no roles. The pipeline therefore publishes the
tables the API serves (`publish.SERVED`, the serving contract: exactly those columns) into a Postgres
`serving` schema. DuckDB does the bulk copy; one Postgres transaction then swaps `serving_next` in,
so a failed or cancelled load never changes what the API reads. Two roles: `loader` can only create
and swap the serving schemas; `api_reader` is read-only with a 5 s statement timeout. The API runs
on Render's free plan and the database on Neon's free plan, both in Frankfurt; both sleep when idle
(the API takes about a minute to wake). The served copy is about 200 MB in Postgres, a fifth of
Neon's free 1 GB, and loads in seconds. Lake and warehouse stay the system of record: the serving
copy can be rebuilt at any time. At fleet scale: managed Postgres or a time-series store, with
row-level security instead of an owner check in code.

## API security
An API key in `X-API-Key` maps to an owner (keys come from env, compared in constant time). Every
vessel endpoint checks ownership: 401 for no key or a bad key, 403 for another owner's vessel, 404
for an unknown vessel. In production: Entra ID / OAuth2 client credentials, owner as a claim,
row-level security in the warehouse, and 404 instead of 403 so vessel IDs can't be enumerated.
