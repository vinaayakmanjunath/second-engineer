# Scaling this to a real fleet

This repo: 3 virtual vessels, about 20 sensors each, 1 Hz, about 80 million readings, one laptop.
A real fleet is hundreds of vessels × hundreds of channels × 1 Hz, which is billions of readings a
month. Here is what changes and what stays.

## Ingestion
- **Stays:** the batch envelope (vessel, batch_id, logger, sent_at, sequence number), sha256 and
  batch_id idempotency, and quarantine with reasons.
- **Changes:** landing becomes an event stream (Azure Event Hubs / IoT Hub) or object-store drops
  with notifications, partitioned by vessel so each ship's order is kept. Ingest runs
  continuously and scales out per partition.
- **Clock handling:** keep the ship clock and the receive clock, and alert on drift between
  `sent_at` and data time per vessel.

## Storage
- Bronze: raw files in ADLS Gen2, kept immutable (lifecycle to cool or archive tiers).
- Silver: Delta or Iceberg tables partitioned by vessel and date, using MERGE instead of
  partition rewrites, with scheduled compaction (small files are the enemy at 1 Hz).
- Hot query path: 1-minute and 1-hour rollups in a time-series store (Azure Data Explorer, or
  MongoDB time-series collections) for dashboards and APIs.
- Retention: raw 1 Hz hot for about 90 days, rollups kept indefinitely.

## Onboarding a vessel in hours
- The mapping is configuration: a UI that suggests canonical sensors from tag names, units and
  value ranges, which an engineer confirms.
- Unmapped-tag and out-of-range counts are the onboarding checklist. The DQ scorecard here is
  that view.
- A data contract per canonical sensor (unit, range, expected rate) is validated at silver.

## Models
- Per vessel and system baselines conditioned on load and context (sea temperature, draft,
  weather), re-learned on a schedule, with each baseline version recorded against the alerts it
  produced.
- Feature tables (hourly residuals per system) give data scientists one reproducible source.
- Alert feedback from engineers (true or false, root cause) becomes the training label set that
  this demo lacks.

## Operations
- SLOs per vessel: freshness (now − last reading), completeness and quarantine rate, with
  alerting when a ship goes quiet or its quality drops.
- Pipeline observability: run duration, partitions or files rewritten, and rows in vs out per
  step. `lake/_runs.jsonl` is the seed of this.
- Multi-tenancy: an identity provider (Entra ID), owner and fleet claims, row-level security in
  the serving store, and audit logs.
