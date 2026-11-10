"""Vessel sensor assignment: different loggers (names, units, clocks) -> one canonical model."""

from datetime import datetime

import polars as pl
import pytest

from secondeng import silver, simulator


def _frames():
    ts = [datetime(2026, 9, 1, 10, 0, 0), datetime(2026, 9, 1, 10, 0, 1)]
    me = pl.DataFrame(
        {"ts_local": ts, "engine_rpm": [700.0, 701.0], "oil_temp_c": [70.0, 71.0], "oil_pressure_bar": [2.5, 2.4]}
    )
    gen = pl.DataFrame({"ts_local": ts, "generator_speed_rpm": [1800.0, 1801.0]})
    return {"main_engine": me, "generator_port": gen}


@pytest.mark.parametrize(
    "vessel_id", ["GLORIA-A", "GLORIA-B"]
)  # gloria_v1 (local, metric) / gloria_v2 (UTC, psi, degF)
def test_both_loggers_map_back_to_the_same_canonical_values(s, vessel_id):
    v = s.vessel(vessel_id)
    recs = simulator.to_logger_records(s, v, _frames())
    lines = (
        recs.with_columns(
            pl.lit(vessel_id).alias("vessel_id"),
            pl.lit("b1").alias("batch_id"),
            pl.lit(v.logger).alias("logger"),
            pl.lit("2026-09-01T16:00:00Z").alias("sent_at"),
            pl.lit(0).alias("seq"),
            pl.lit("f").alias("file_name"),
        )
        .rename({"tags": "tags", "values": "values"})
        .drop("ts_utc")
    )
    rows = silver.classify(s, lines, silver.tag_table(s))
    assert rows["reason"].is_null().all()
    got = {(r["sensor_id"], r["ts_utc"]): r["value"] for r in rows.iter_rows(named=True)}
    # ship clock is America/Bogota (UTC-5) -> 15:00 UTC
    assert got[("me.lo_temp_c", datetime(2026, 9, 1, 15, 0, 0))] == pytest.approx(70.0, abs=0.01)
    assert got[("me.lo_press_bar", datetime(2026, 9, 1, 15, 0, 1))] == pytest.approx(2.4, abs=0.01)
    assert got[("dg1.speed_rpm", datetime(2026, 9, 1, 15, 0, 0))] == pytest.approx(1800.0, abs=0.01)
    if v.logger == "gloria_v2":
        assert "ME_LO_T_F" in rows["tag"].to_list()  # raw tag really was the vendor name in degF
