"""Bronze idempotency, silver dedupe / late data / ordering / quarantine."""

import gzip
from datetime import date, datetime

import polars as pl
import pytest

from secondeng import bronze, lake, silver
from secondeng.settings import Logger
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


def test_truncated_gzip_is_recorded_corrupt_and_does_not_block_other_files(s):
    good = _batch(
        s.landing, "20260901T151000_b1.jsonl.gz", "b1", "2026-09-01T15:10:00Z", [("2026-09-01 10:00:00", [700.0, 70.0])]
    )
    cut = s.landing / "GLORIA-A" / "20260901T152700_b2.jsonl.gz"
    cut.write_bytes(good.read_bytes()[:-20])  # transfer cut off mid-file
    bronze.ingest(s)
    man = bronze.read_manifest(s)
    assert dict(zip(man["file_name"], man["status"], strict=True)) == {good.name: "loaded", cut.name: "corrupt_file"}
    assert bronze.ingest(s)["new_files"] == 0
    silver.run(s)
    assert _points(s).height == 2


def test_malformed_line_isolates_its_file_and_a_later_good_copy_still_loads(s):
    row = [("2026-09-01 10:00:00", [700.0, 70.0])]
    _batch(s.landing, "20260901T151000_b1.jsonl.gz", "b1", "2026-09-01T15:10:00Z", row)
    bad = _batch(
        s.landing, "20260901T161000_b2.jsonl.gz", "b2", "2026-09-01T16:10:00Z", [("2026-09-01 11:00:00", [1, 2])]
    )
    bad.write_bytes(gzip.compress(gzip.decompress(bad.read_bytes()) + b'{"vessel_id": "GLORIA-A", broken\n', mtime=0))
    bronze.ingest(s)
    silver.run(s)
    log = silver.read_log(s, "batches")
    assert dict(zip(log["file_name"], log["status"], strict=True))["20260901T161000_b2.jsonl.gz"] == "unparseable"
    assert _points(s).height == 2
    # the sender re-sends b2 intact: it must load, not be mistaken for a duplicate of the broken copy
    _batch(
        s.landing, "20260901T170000_b2.jsonl.gz", "b2", "2026-09-01T17:00:00Z", [("2026-09-01 11:00:00", [701.0, 71.0])]
    )
    bronze.ingest(s)
    silver.run(s)
    assert _points(s).height == 4


def test_crash_before_manifest_update_does_not_double_count(s, monkeypatch):
    lines = [
        line(
            "GLORIA-A", "b1", "2026-09-01T15:10:00Z", 0, "main_engine", "2026-09-01 10:00:00", ["engine_rpm"], [700.0]
        ),
        line("GLORIA-A", "b1", "2026-09-01T15:10:00Z", 1, "main_engine", "", ["engine_rpm"], [700.0]),
    ]
    write_batch(s.landing, "20260901T151000_b1.jsonl.gz", lines)
    bronze.ingest(s)
    clock = iter(datetime(2026, 11, 10, 12, 0, i) for i in range(100))
    monkeypatch.setattr(lake, "now_utc", lambda: next(clock))
    real = lake.write_parquet_atomic

    def crash_on_manifest(df, path):
        if path.name == "_manifest.parquet":
            raise RuntimeError("power cut")
        return real(df, path)

    monkeypatch.setattr(lake, "write_parquet_atomic", crash_on_manifest)
    with pytest.raises(RuntimeError):
        silver.run(s)
    monkeypatch.setattr(lake, "write_parquet_atomic", real)
    silver.run(s)  # re-processes the same file
    assert int(silver.read_log(s, "quarantine")["n_rows"].sum()) == 1
    assert silver.read_log(s, "batches").height == 1
    assert _points(s).height == 1


def test_clock_problems_are_quarantined_with_their_own_reason(s):
    oslo = Logger(timezone="Europe/Oslo", sources=s.loggers["gloria_v1"].sources)
    s = s.model_copy(update={"loggers": {**s.loggers, "oslo_v1": oslo}})
    sent = "2026-10-25T06:00:00Z"
    rpm = ["engine_rpm"]
    lines = [
        line(
            "GLORIA-A", "b1", sent, 0, "main_engine", "2026-10-25 02:30:00", rpm, [700.0], logger="oslo_v1"
        ),  # DST overlap
        line(
            "GLORIA-A", "b1", sent, 1, "main_engine", "2026-03-29 02:30:00", rpm, [700.0], logger="oslo_v1"
        ),  # DST gap
        line("GLORIA-A", "b1", sent, 2, "main_engine", "2026-10-25 01:00:00", rpm, [700.0], logger="nope_v9"),
        line("GLORIA-A", "b1", sent, 3, "main_engine", "1970-01-01 00:00:00", rpm, [700.0]),  # logger clock reset
        line("GLORIA-A", "b1", sent, 4, "main_engine", "2026-10-25 00:00:00", rpm, [700.0]),
    ]
    write_batch(s.landing, "20261025T060000_b1.jsonl.gz", lines)
    bronze.ingest(s)
    r = silver.run(s)
    q = silver.read_log(s, "quarantine")
    assert dict(zip(q["reason"], q["n_rows"], strict=True)) == {
        "dst_transition": 2,
        "unknown_logger": 1,
        "stale_timestamp": 1,
    }
    assert r["points_valid"] == 1


def test_newest_sent_version_of_a_reading_wins_regardless_of_arrival_order(s, monkeypatch):
    clock = iter(datetime(2026, 11, 10, 12, 0, i) for i in range(100))
    monkeypatch.setattr(lake, "now_utc", lambda: next(clock))
    _batch(
        s.landing, "20260901T160000_b2.jsonl.gz", "b2", "2026-09-01T16:00:00Z", [("2026-09-01 10:00:00", [710.0, 71.0])]
    )
    bronze.ingest(s)
    silver.run(s)
    # an older version of the same reading (sent earlier) arrives late
    _batch(
        s.landing, "20260901T151000_b1.jsonl.gz", "b1", "2026-09-01T15:10:00Z", [("2026-09-01 10:00:00", [700.0, 70.0])]
    )
    bronze.ingest(s)
    silver.run(s)
    rpm = _points(s).filter(pl.col("sensor_id") == "me.speed_rpm")
    assert rpm["value"].to_list() == [710.0]
