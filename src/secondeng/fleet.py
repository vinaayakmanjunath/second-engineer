"""Virtual fleet: split the ARC Gloria export into vessels by voyage legs, shift to the replay date."""

from __future__ import annotations

from datetime import datetime, timedelta
from functools import cache

import polars as pl

from secondeng.settings import Settings, Vessel

GLORIA_DIR = "gloria/main_machinery_sensored"
SOURCES = {
    "main_engine": "main_engine.csv",
    "generator_port": "generator_port.csv",
    "generator_starboard": "generator_starboard.csv",
}


@cache
def _read(path: str) -> pl.DataFrame:
    df = pl.read_csv(path, infer_schema_length=10000)
    # main_engine uses "YYYY-MM-DD HH:MM:SS", generators "YYYY-MM-DDTHH:MM:SS"
    return df.with_columns(pl.col("timestamp").str.replace("T", " ").str.to_datetime("%Y-%m-%d %H:%M:%S"))


def read_source(s: Settings, source: str) -> pl.DataFrame:
    return _read(str(s.raw / GLORIA_DIR / SOURCES[source]))


def leg_spans(s: Settings) -> pl.DataFrame:
    return (
        read_source(s, "main_engine")
        .group_by("navigation_leg")
        .agg(pl.col("timestamp").min().alias("start"), pl.col("timestamp").max().alias("end"))
    )


def vessel_window(s: Settings, v: Vessel) -> tuple[datetime, datetime, timedelta]:
    spans = leg_spans(s).filter(pl.col("navigation_leg").is_in(v.legs))
    if spans.height != len(v.legs):
        raise ValueError(f"{v.vessel_id}: unknown leg in {v.legs}")
    start, end = spans["start"].min(), spans["end"].max()
    shift = timedelta(days=(s.fleet.replay_start - start.date()).days)
    return start, end, shift


def load_vessel(s: Settings, v: Vessel) -> dict[str, pl.DataFrame]:
    """Per source: ts_local (shifted, naive local ship time) + raw Gloria columns."""
    start, end, shift = vessel_window(s, v)
    out = {}
    for source in SOURCES:
        df = read_source(s, source).filter(pl.col("timestamp").is_between(start, end))
        out[source] = df.select(
            (pl.col("timestamp") + shift).alias("ts_local"),
            pl.exclude("timestamp", "navigation_leg"),
        )
    return out
