"""Inject a bench fault signature into one vessel's main engine as a linear ramp.

value_injected = value * (1 + severity * rel_dev * ramp(t)) (+ mild noise), ramp 0 -> 1 over ramp_hours,
ending at the declared failure time; only while the engine is running. Main-engine rows after
the failure are dropped (engine down). Ground truth goes to data/ground_truth/ and is read only
by evaluate.py, never by the pipeline.
"""

from __future__ import annotations

import json
from datetime import timedelta

import numpy as np
import polars as pl

from secondeng.settings import Settings
from secondeng.signatures import mean_signature


def plan(s: Settings, me: pl.DataFrame) -> dict:
    inj = s.injection
    first = me["ts_local"].min()
    failure = first + timedelta(days=inj.failure_offset_days)
    ramp_start = failure - timedelta(hours=inj.ramp_hours)
    tz = s.loggers[s.vessel(inj.vessel_id).logger].timezone
    return {
        "vessel_id": inj.vessel_id,
        "system": "ME",
        "fault": inj.fault,
        "ramp_start_local": ramp_start.isoformat(),
        "failure_local": failure.isoformat(),
        "timezone": tz,
        "ramp_hours": inj.ramp_hours,
        "severity_x_bench": inj.severity_x_bench,
    }


def apply(s: Settings, me: pl.DataFrame, sig: pl.DataFrame, gt: dict) -> pl.DataFrame:
    vec = {
        m: d * s.injection.severity_x_bench
        for m, d in mean_signature(sig)
        .filter(pl.col("fault") == gt["fault"])
        .select("measurement", "rel_dev")
        .iter_rows()
    }
    if not vec:
        raise ValueError(f"no signature for fault {gt['fault']}")
    tags = s.loggers["gloria_v1"].sources["main_engine"]
    rpm_col = next(c for c, t in tags.items() if t.sensor == "me.speed_rpm")
    ramp_start = pl.lit(gt["ramp_start_local"]).str.to_datetime()
    failure = pl.lit(gt["failure_local"]).str.to_datetime()
    me = me.filter(pl.col("ts_local") < failure)
    ramp = ((pl.col("ts_local") - ramp_start).dt.total_seconds() / (s.injection.ramp_hours * 3600)).clip(0, 1)
    p99 = me[rpm_col].quantile(0.99)
    running = pl.col(rpm_col) > s.detection.running_frac_of_p99_rpm * p99
    rng = np.random.default_rng(s.seed)
    noise = pl.Series(rng.normal(0, s.injection.noise_frac, me.height))
    exprs = []
    for col, t in tags.items():
        rel = vec.get(s.sensors[t.sensor].measurement)
        if rel is None or col not in me.columns:
            continue
        factor = 1 + rel * ramp + pl.when(ramp > 0).then(pl.lit(noise)).otherwise(0)
        exprs.append(pl.when(running).then(pl.col(col) * factor).otherwise(pl.col(col)).alias(col))
    return me.with_columns(exprs)


def inject(s: Settings, frames: dict[str, pl.DataFrame], sig: pl.DataFrame) -> dict[str, pl.DataFrame]:
    gt = plan(s, frames["main_engine"])
    frames = frames | {"main_engine": apply(s, frames["main_engine"], sig, gt)}
    path = s.path(s.ground_truth)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([gt], indent=2))
    return frames
