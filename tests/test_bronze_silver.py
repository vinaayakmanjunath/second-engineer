"""Bronze idempotency, silver dedupe / late data / ordering / quarantine."""

from datetime import date

import polars as pl

from secondeng import bronze, silver
from tests.conftest import line, write_batch

ME_TAGS = ["engine_rpm", "oil_temp_c"]


def _batch(landing, name, batch_id, sent_at, rows, seq_order=None):
    lines = [
        line("GLORIA-A", batch_id, sent_at, i, "main_engine", ts, ME_TAGS, vals) for i, (ts, vals) in enumerate(rows)
    ]
    if seq_order:
        lines = [lines[i] for i in seq_order]
    return write_batch(landing, name, lines)


def _points(s):
    return pl.read_parquet(s.lake / "silver" / "points" / "**" / "*.parquet", hive_partitioning=True)


def test_bronze_is_idempotent_and_catches_identical_resend(s):
    p = _batch(
        s.landing, "20260901T151000_b1.jsonl.gz", "b1", "2026-09-01T15:10:00Z", [("2026-09-01 10:00:00", [700.0, 70.0])]
    )
    (s.landing / "GLORIA-A" / "20260901T152700_b1.jsonl.gz").write_bytes(p.read_bytes())  # same bytes, new name
    first = bronze.ingest(s)
    m1 = bronze.read_manifest(s)
    second = bronze.ingest(s)
    assert first["files_loaded"] == 1 and first["files_duplicate_file"] == 1
    assert second["new_files"] == 0
    assert m1.equals(bronze.read_manifest(s))


def test_resend_with_new_envelope_adds_no_rows(s):
    rows = [("2026-09-01 10:00:00", [700.0, 70.0]), ("2026-09-01 10:00:01", [701.0, 70.1])]
    _batch(s.landing, "20260901T151000_b1.jsonl.gz", "b1", "2026-09-01T15:10:00Z", rows)
    _batch(s.landing, "20260901T152700_b1.jsonl.gz", "b1", "2026-09-01T15:27:00Z", rows)  # different bytes
    bronze.ingest(s)
    r = silver.run(s)
    assert r["duplicate_batches"] == 1
    assert _points(s).height == 4  # 2 records x 2 sensors, once


def test_late_batch_rewrites_only_its_partition_and_output_is_sorted(s):
    _batch(
        s.landing, "20260901T151000_b1.jsonl.gz", "b1", "2026-09-01T15:10:00Z", [("2026-09-01 10:00:00", [700.0, 70.0])]
    )
    _batch(
        s.landing, "20260902T151000_b2.jsonl.gz", "b2", "2026-09-02T15:10:00Z", [("2026-09-02 10:00:00", [700.0, 70.0])]
    )
    bronze.ingest(s)
    silver.run(s)
    day2 = silver.partition_path(s, "GLORIA-A", date(2026, 9, 2))
    before = day2.stat().st_mtime_ns
    # a batch for day 1 arrives a day late, with its rows shuffled in transit
    _batch(
        s.landing,
        "20260902T200000_b3.jsonl.gz",
        "b3",
        "2026-09-02T20:00:00Z",
        [
            ("2026-09-01 10:05:00", [705.0, 70.5]),
            ("2026-09-01 10:05:01", [706.0, 70.6]),
            ("2026-09-01 10:05:02", [707.0, 70.7]),
        ],
        seq_order=[2, 0, 1],
    )
    bronze.ingest(s)
    r = silver.run(s)
    assert r["partitions"] == ["GLORIA-A/2026-09-01"]
    assert day2.stat().st_mtime_ns == before
    day1 = pl.read_parquet(silver.partition_path(s, "GLORIA-A", date(2026, 9, 1)))
    assert day1.height == 8
    assert day1.equals(day1.sort("sensor_id", "ts_utc"))
    log = pl.read_parquet(s.lake / "silver" / "batches" / "*.parquet")
    # arrival order 2,0,1: one order break (0 arrives right after 2)
    assert log.filter(pl.col("batch_id") == "b3")["out_of_order_rows"].item() == 1


def test_each_quarantine_reason_triggers(s):
    lines = [
        line("GLORIA-A", "b1", "2026-09-01T15:10:00Z", 0, "main_engine", "", ["engine_rpm"], [700.0]),
        line(
            "GLORIA-A", "b1", "2026-09-01T15:10:00Z", 1, "main_engine", "2026-09-03 10:00:00", ["engine_rpm"], [700.0]
        ),
        line("GLORIA-A", "b1", "2026-09-01T15:10:00Z", 2, "main_engine", "2026-09-01 10:00:00", ["mystery_tag"], [1.0]),
        line("GLORIA-A", "b1", "2026-09-01T15:10:00Z", 3, "main_engine", "2026-09-01 10:00:01", ["engine_rpm"], [None]),
        line("GLORIA-A", "b1", "2026-09-01T15:10:00Z", 4, "main_engine", "2026-09-01 10:00:02", ["engine_rpm"], [-5.0]),
        line(
            "GLORIA-A", "b1", "2026-09-01T15:10:00Z", 5, "main_engine", "2026-09-01 10:00:03", ["engine_rpm"], [700.0]
        ),
    ]
    write_batch(s.landing, "20260901T151000_b1.jsonl.gz", lines)
    bronze.ingest(s)
    r = silver.run(s)
    q = pl.read_parquet(s.lake / "silver" / "quarantine" / "*.parquet")
    assert set(q["reason"]) == {"bad_timestamp", "future_timestamp", "unknown_sensor", "missing_value", "out_of_range"}
    assert r["points_valid"] == 1 and r["points_quarantined"] == 5
