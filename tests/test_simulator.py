from datetime import datetime, timedelta

import numpy as np
import polars as pl

from secondeng import simulator


def _recs(hours=24 * 60):
    ts = [datetime(2026, 9, 1) + timedelta(minutes=m) for m in range(0, hours * 60, 5)]
    return pl.DataFrame({"ts_utc": ts}).with_columns(pl.lit("main_engine").alias("source"))


def test_plan_is_deterministic_for_a_seed(s):
    v = s.vessel("GLORIA-A")
    a = simulator.plan_batches(s, v, _recs(), np.random.default_rng(1))
    b = simulator.plan_batches(s, v, _recs(), np.random.default_rng(1))
    assert a.equals(b)


def test_mess_rates_within_tolerance(s):
    v = s.vessel("GLORIA-A")
    plan = simulator.plan_batches(s, v, _recs(), np.random.default_rng(7))
    orig = plan.filter(pl.col("copy") == "original")
    n = orig.height  # 60 days of 15-minute batches
    assert n == 60 * 96
    dups = plan.height - n
    assert abs(dups / n - s.simulator.duplicate_rate) < 0.01
    assert abs(orig["late"].mean() - s.simulator.late_rate) < 0.015
    assert (orig["sent_at"] >= orig["window_end"]).all()  # nothing is sent before its window closes
