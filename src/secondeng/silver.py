"""Silver: parse bronze batches -> typed, UTC, canonical long format, deduped, quarantined.

Incremental: only new bronze files are parsed, and only the (vessel_id, date) partitions they
touch are rewritten (read existing + new -> dedupe -> atomic replace). A batch_id already loaded
from a different file is a duplicate and is skipped. Rows that fail validation go to quarantine
(aggregated counts per vessel/hour/tag/reason) instead of being dropped silently.
"""

from __future__ import annotations

import gzip
from datetime import datetime
from pathlib import Path

import polars as pl

from secondeng import bronze, lake
from secondeng.settings import Settings
from secondeng.simulator import LINE_SCHEMA

CHUNK_FILES = 400
FUTURE_TOLERANCE = pl.duration(minutes=5)
POINT_COLUMNS = ["ts_utc", "sensor_id", "value", "batch_id", "sent_at", "ingested_at"]


def points_dir(s: Settings) -> Path:
    return s.lake / "silver" / "points"


def partition_path(s: Settings, vessel_id: str, day) -> Path:
    return points_dir(s) / f"vessel_id={vessel_id}" / f"date={day}" / "part-0.parquet"


def tag_table(s: Settings) -> pl.DataFrame:
    rows = [
        {
            "logger": lg,
            "source": src,
            "tag": tag,
            "sensor_id": t.sensor,
            "scale": t.scale,
            "offset": t.offset,
            "vmin": s.sensors[t.sensor].min,
            "vmax": s.sensors[t.sensor].max,
        }
        for lg, L in s.loggers.items()
        for src, tags in L.sources.items()
        for tag, t in tags.items()
    ]
    return pl.DataFrame(rows)


def loaded_batches(s: Settings) -> dict[str, str]:
    """batch_id -> file_name for batches already in silver."""
    d = s.lake / "silver" / "batches"
    if not any(d.glob("*.parquet")):
        return {}
    log = pl.read_parquet(d / "*.parquet").filter(pl.col("status") == "loaded")
    return dict(log.select("batch_id", "file_name").unique("batch_id", keep="first").iter_rows())


def parse_ts(s: Settings) -> pl.Expr:
    naive = (
        pl.col("ts")
        .str.replace("T", " ")
        .str.strip_suffix("Z")
        .str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False, time_unit="us")
    )
    expr = None
    for name, lg in s.loggers.items():
        utc = naive.dt.replace_time_zone(lg.timezone, non_existent="null").dt.convert_time_zone("UTC")
        expr = (
            pl.when(pl.col("logger") == name).then(utc)
            if expr is None
            else expr.when(pl.col("logger") == name).then(utc)
        )
    return expr.otherwise(None).dt.replace_time_zone(None).alias("ts_utc")


def read_files(s: Settings, files: pl.DataFrame) -> pl.DataFrame:
    frames = []
    for fname, path in files.select("file_name", "bronze_path").iter_rows():
        raw = gzip.decompress((s.lake / path).read_bytes())
        frames.append(
            pl.read_ndjson(raw, schema=LINE_SCHEMA)
            .with_row_index("line")
            .with_columns(pl.lit(fname).alias("file_name"))
        )
    return pl.concat(frames)


def classify(s: Settings, lines: pl.DataFrame, tags: pl.DataFrame) -> pl.DataFrame:
    """Explode records to one row per reading, map to canonical sensors, attach a quarantine reason."""
    return (
        lines.with_columns(
            parse_ts(s),
            pl.col("sent_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ", time_unit="us"),
        )
        .explode("tags", "values")
        .rename({"tags": "tag", "values": "raw_value"})
        .join(tags, on=["logger", "source", "tag"], how="left")
        .with_columns((pl.col("raw_value") * pl.col("scale") + pl.col("offset")).alias("value"))
        .with_columns(
            pl.when(pl.col("ts_utc").is_null())
            .then(pl.lit("bad_timestamp"))
            .when(pl.col("ts_utc") > pl.col("sent_at") + FUTURE_TOLERANCE)
            .then(pl.lit("future_timestamp"))
            .when(pl.col("sensor_id").is_null())
            .then(pl.lit("unknown_sensor"))
            .when(pl.col("value").is_null() | pl.col("value").is_nan())
            .then(pl.lit("missing_value"))
            .when((pl.col("value") < pl.col("vmin")) | (pl.col("value") > pl.col("vmax")))
            .then(pl.lit("out_of_range"))
            .otherwise(None)
            .alias("reason")
        )
    )


def batch_log(files: pl.DataFrame, lines: pl.DataFrame, rows: pl.DataFrame, ingested_at: datetime) -> pl.DataFrame:
    # a record is out of order if it arrived right after a record the logger produced later (higher seq)
    ooo = (
        lines.sort("file_name", "line")
        .with_columns((pl.col("seq") < pl.col("seq").shift(1).over("file_name")).alias("ooo"))
        .group_by("file_name")
        .agg(pl.col("ooo").sum().alias("out_of_order_rows"))
    )
    stats = rows.group_by("file_name").agg(
        pl.len().alias("n_points"),
        pl.col("reason").is_null().sum().alias("n_valid"),
        (pl.col("reason") == "unknown_sensor").sum().alias("n_unmapped"),
        pl.col("ts_utc").filter(pl.col("reason").is_null()).min().alias("min_ts_utc"),
        pl.col("ts_utc").filter(pl.col("reason").is_null()).max().alias("max_ts_utc"),
    )
    return (
        files.select("file_name", "vessel_id", "batch_id", "logger", "sent_at", "n_lines", "status")
        .join(stats, on="file_name", how="left")
        .join(ooo, on="file_name", how="left")
        .with_columns(pl.lit(ingested_at).alias("ingested_at"))
    )


def merge_partitions(s: Settings, valid: pl.DataFrame) -> tuple[list[str], int]:
    """Rewrite only the touched (vessel, date) partitions. Latest ingest wins on (sensor, ts)."""
    rewritten, dropped = [], 0
    for (vessel_id, day), new in valid.group_by("vessel_id", pl.col("ts_utc").dt.date().alias("date")):
        path = partition_path(s, vessel_id, day)
        new = new.select(POINT_COLUMNS)
        both = pl.concat([pl.read_parquet(path), new]) if path.exists() else new
        merged = (
            both.sort("ingested_at", "sent_at")
            .unique(subset=["sensor_id", "ts_utc"], keep="last", maintain_order=False)
            .sort("sensor_id", "ts_utc")
        )
        dropped += both.height - merged.height
        lake.write_parquet_atomic(merged, path)
        rewritten.append(f"{vessel_id}/{day}")
    return rewritten, dropped


def run(s: Settings) -> dict:
    t0 = lake.now_utc()
    ingested_at = t0.replace(microsecond=0)
    man = bronze.read_manifest(s)
    todo = man.filter((pl.col("status") == "loaded") & pl.col("silvered_at").is_null()).sort("sent_at", "file_name")
    known = loaded_batches(s)
    tags = tag_table(s)
    totals = {
        "files": 0,
        "duplicate_batches": 0,
        "points_valid": 0,
        "points_quarantined": 0,
        "rows_deduped": 0,
        "partition_writes": 0,
    }
    touched: set[str] = set()
    for i in range(0, todo.height, CHUNK_FILES):
        files = todo.slice(i, CHUNK_FILES)
        # a batch_id already loaded from another file (or earlier in this chunk) is a re-send
        status = []
        for fname, bid in files.select("file_name", "batch_id").iter_rows():
            if bid in known and known[bid] != fname:
                status.append("duplicate_batch")
            else:
                known[bid] = fname
                status.append("loaded")
        files = files.with_columns(pl.Series("status", status))
        load = files.filter(pl.col("status") == "loaded")
        lines = (
            read_files(s, load)
            if load.height
            else pl.DataFrame(schema={"line": pl.UInt32} | LINE_SCHEMA | {"file_name": pl.String})
        )
        rows = classify(s, lines, tags)
        valid = rows.filter(pl.col("reason").is_null()).with_columns(pl.lit(ingested_at).alias("ingested_at"))
        rewritten, dropped = merge_partitions(s, valid)
        bad = rows.filter(pl.col("reason").is_not_null())
        quarantine = bad.group_by(
            "vessel_id",
            pl.col("sent_at").dt.truncate("1h").alias("sent_hour"),
            "logger",
            "source",
            "tag",
            "sensor_id",
            "reason",
        ).agg(
            pl.len().alias("n_rows"),
            pl.col("ts").first().alias("sample_ts"),
            pl.col("raw_value").first().alias("sample_value"),
        )
        part = f"part-{ingested_at:%Y%m%dT%H%M%S}-{i // CHUNK_FILES:04d}.parquet"
        lake.write_parquet_atomic(
            quarantine.with_columns(pl.lit(ingested_at).alias("ingested_at")), s.lake / "silver" / "quarantine" / part
        )
        lake.write_parquet_atomic(batch_log(files, lines, rows, ingested_at), s.lake / "silver" / "batches" / part)
        # mark files done last: a crash before this line re-processes them idempotently
        man = man.with_columns(
            pl.when(pl.col("file_name").is_in(files["file_name"].implode()))
            .then(pl.lit(ingested_at))
            .otherwise(pl.col("silvered_at"))
            .alias("silvered_at")
        )
        lake.write_parquet_atomic(man, bronze.manifest_path(s))
        totals["files"] += files.height
        totals["duplicate_batches"] += files.height - load.height
        totals["points_valid"] += valid.height
        totals["points_quarantined"] += bad.height
        totals["rows_deduped"] += dropped
        totals["partition_writes"] += len(rewritten)
        touched.update(rewritten)
    secs = (lake.now_utc() - t0).total_seconds()
    return lake.log_run(
        s.lake,
        "silver",
        seconds=round(secs, 2),
        partitions_rewritten=len(touched),
        partitions=sorted(touched),
        **totals,
    )
