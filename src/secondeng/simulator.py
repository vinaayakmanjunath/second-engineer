"""Ship-to-shore simulator: replay each vessel as satellite batches into landing/.

One gzip JSON-lines file per batch; every line is one logger record:
  {vessel_id, batch_id, logger, sent_at, seq, source, ts, tags: [...], values: [...]}
Raw logger tags, units and clock (the pipeline must map and convert). Transport mess is seeded
and deterministic: identical re-sends, re-sends with a new envelope, late batches, outages with
catch-up bursts, out-of-order rows, and a few corrupt timestamps.
"""

from __future__ import annotations

import gzip
import io
import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl

from secondeng import fleet, inject, signatures
from secondeng.settings import Settings, Vessel

# The published Gloria export: main engine uses a space, generators a 'T' (kept on purpose).
V1_TS_FORMAT = {"main_engine": "%Y-%m-%d %H:%M:%S"}
DEFAULT_V1_TS_FORMAT = "%Y-%m-%dT%H:%M:%S"
LINE_SCHEMA = {
    "vessel_id": pl.String,
    "batch_id": pl.String,
    "logger": pl.String,
    "sent_at": pl.String,
    "seq": pl.Int64,
    "source": pl.String,
    "ts": pl.String,
    "tags": pl.List(pl.String),
    "values": pl.List(pl.Float64),
}


def to_logger_records(s: Settings, v: Vessel, frames: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Gloria columns -> this vessel's logger tags/units/clock. One row per record."""
    v1 = s.loggers["gloria_v1"]
    lg = s.loggers[v.logger]
    parts = []
    for source, df in frames.items():
        sensor_of = {col: t.sensor for col, t in v1.sources[source].items()}
        tag_of = {t.sensor: (tag, t) for tag, t in lg.sources[source].items()}
        cols = [c for c in df.columns if c in sensor_of]
        tags, vals = [], []
        for c in cols:
            tag, t = tag_of[sensor_of[c]]
            tags.append(tag)
            vals.append(((pl.col(c) - t.offset) / t.scale).round(3))
        utc_wall = pl.col("ts_local").dt.replace_time_zone(v1.timezone).dt.convert_time_zone("UTC")
        if lg.timezone == "UTC":
            ts = utc_wall.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            ts = pl.col("ts_local").dt.strftime(V1_TS_FORMAT.get(source, DEFAULT_V1_TS_FORMAT))
        parts.append(
            df.select(
                utc_wall.dt.replace_time_zone(None).alias("ts_utc"),
                pl.lit(source).alias("source"),
                ts.alias("ts"),
                pl.lit(tags, dtype=pl.List(pl.String)).alias("tags"),
                pl.concat_list(vals).alias("values"),
            )
        )
    return pl.concat(parts).sort("ts_utc", "source")


def plan_batches(s: Settings, v: Vessel, recs: pl.DataFrame, rng: np.random.Generator) -> pl.DataFrame:
    """One row per file to send: batch window, sent_at and the transport mess applied."""
    sim = s.simulator
    every = f"{sim.batch_minutes}m"
    b = (
        recs.group_by(pl.col("ts_utc").dt.truncate(every).alias("window_start"))
        .agg(pl.len().alias("n_lines"))
        .sort("window_start")
        .with_columns(
            (pl.col("window_start") + pl.duration(minutes=sim.batch_minutes)).alias("window_end"),
            (pl.lit(v.vessel_id) + "-" + pl.col("window_start").dt.strftime("%Y%m%dT%H%M")).alias("batch_id"),
        )
    )
    n = b.height
    delay = rng.uniform(*sim.send_delay_minutes, n)
    late = rng.random(n) < sim.late_rate
    late_h = np.where(late, rng.uniform(*sim.late_hours, n), 0.0)
    sent = (
        b["window_end"].to_numpy()
        + (delay * 60e6).astype("timedelta64[us]")
        + (late_h * 3600e6).astype("timedelta64[us]")
    )
    # outages: no link for a while, then everything queued is sent in a burst
    in_outage = np.zeros(n, bool)
    t0, t1 = b["window_start"].min(), b["window_end"].max()
    span_h = (t1 - t0).total_seconds() / 3600
    for _ in range(sim.outages_per_vessel):
        dur = rng.uniform(*sim.outage_hours)
        start = np.datetime64(t0 + timedelta(hours=rng.uniform(0, max(span_h - dur, 1))), "us")
        end = start + np.timedelta64(int(dur * 3600e6), "us")
        hit = (sent >= start) & (sent < end)
        sent = np.where(hit, end + (np.arange(n) % 60 * 1e6).astype("timedelta64[us]"), sent)
        in_outage |= hit
    b = b.with_columns(
        pl.Series("sent_at", sent).cast(pl.Datetime("us")).dt.truncate("1s"),
        pl.Series("late", late),
        pl.Series("in_outage", in_outage),
        pl.lit("original").alias("copy"),
    )
    dup = rng.random(n) < sim.duplicate_rate
    identical = rng.random(n) < 0.5
    dups = b.filter(pl.Series(dup)).with_columns(
        pl.col("sent_at") + pl.duration(minutes=17),
        pl.Series(np.where(identical[dup], "resend_identical", "resend_new_envelope")).alias("copy"),
    )
    return pl.concat([b, dups]).sort("sent_at", "batch_id")


def render(
    batch_recs: pl.DataFrame,
    row: dict,
    v: Vessel,
    rng: np.random.Generator,
    envelope_sent_at: datetime,
    ooo_rate: float,
    corrupt_rate: float,
    stats: Counter,
) -> bytes:
    df = batch_recs.with_row_index("seq")
    n = df.height
    order = np.arange(n)
    k = int(rng.binomial(n, ooo_rate))
    if k > 1:
        idx = rng.choice(n, k, replace=False)
        order[idx] = rng.permutation(order[idx])
        stats["ooo_rows"] += k
    df = df.select(pl.all().gather(order))
    c = rng.random(n) < corrupt_rate
    if c.any():
        blank = c & (rng.random(n) < 0.5)
        future = c & ~blank
        n_tags = df["tags"].list.len().to_numpy()
        stats["corrupt_ts_blank_readings"] += int(n_tags[blank].sum())
        stats["corrupt_ts_future_readings"] += int(n_tags[future].sum())
        ts = df["ts"].to_numpy().astype(object)
        ts[blank] = ""
        ts[future] = [_plus_one_day(t) for t in ts[future]]
        df = df.with_columns(pl.Series("ts", ts, dtype=pl.String))
    out = df.select(
        pl.lit(v.vessel_id).alias("vessel_id"),
        pl.lit(row["batch_id"]).alias("batch_id"),
        pl.lit(v.logger).alias("logger"),
        pl.lit(envelope_sent_at.strftime("%Y-%m-%dT%H:%M:%SZ")).alias("sent_at"),
        pl.col("seq").cast(pl.Int64),
        "source",
        "ts",
        "tags",
        "values",
    )
    buf = io.BytesIO()
    out.write_ndjson(buf)
    return gzip.compress(buf.getvalue(), compresslevel=6, mtime=0)


def _plus_one_day(ts: str) -> str:
    fmt = "%Y-%m-%dT%H:%M:%SZ" if ts.endswith("Z") else ("%Y-%m-%dT%H:%M:%S" if "T" in ts else "%Y-%m-%d %H:%M:%S")
    return (datetime.strptime(ts, fmt) + timedelta(days=1)).strftime(fmt)


def simulate(s: Settings, until: datetime | None = None) -> dict:
    sig = signatures.write(s)
    log: dict = {"until_utc": until.isoformat() if until else None, "vessels": {}}
    for i, v in enumerate(s.fleet.vessels):
        rng = np.random.default_rng(s.seed + i)
        frames = fleet.load_vessel(s, v)
        if v.vessel_id == s.injection.vessel_id:
            frames = inject.inject(s, frames, sig)
        recs = to_logger_records(s, v, frames)
        plan = plan_batches(s, v, recs, rng)
        recs = recs.with_columns(pl.col("ts_utc").dt.truncate(f"{s.simulator.batch_minutes}m").alias("_w"))
        by_window = {k[0]: g.drop("_w", "ts_utc") for k, g in recs.group_by("_w")}
        stats: Counter = Counter()
        rendered: dict[str, bytes] = {}
        out_dir = s.landing / v.vessel_id
        out_dir.mkdir(parents=True, exist_ok=True)
        for row in plan.iter_rows(named=True):
            stats[f"files_{row['copy']}"] += 1
            stats["late_batches"] += int(row["late"] and row["copy"] == "original")
            stats["outage_batches"] += int(row["in_outage"] and row["copy"] == "original")
            if row["copy"] == "resend_identical":
                body = rendered[row["batch_id"]]
            else:
                body = render(
                    by_window[row["window_start"]],
                    row,
                    v,
                    rng,
                    row["sent_at"],
                    s.simulator.out_of_order_row_rate if row["copy"] == "original" else 0.0,
                    s.simulator.corrupt_row_rate if row["copy"] == "original" else 0.0,
                    stats if row["copy"] == "original" else Counter(),
                )
                rendered.setdefault(row["batch_id"], body)
            if until is not None and row["sent_at"] > until:
                continue
            path = out_dir / f"{row['sent_at']:%Y%m%dT%H%M%S}_{row['batch_id']}.jsonl.gz"
            if not path.exists():
                tmp = path.with_name("." + path.name)
                tmp.write_bytes(body)
                tmp.rename(path)
                stats["files_written"] += 1
            stats["files_in_landing"] += 1
        stats["records"] = recs.height
        stats["readings"] = int(recs["tags"].list.len().sum())
        stats["batches"] = plan.filter(pl.col("copy") == "original").height
        stats["first_ts_utc"] = str(recs["ts_utc"].min())
        stats["last_ts_utc"] = str(recs["ts_utc"].max())
        log["vessels"][v.vessel_id] = dict(stats)
    (s.landing / "_sim_log.json").write_text(json.dumps(log, indent=2))
    return log


def landing_files(landing: Path) -> list[Path]:
    return sorted(p for p in landing.glob("*/*.jsonl.gz"))
