"""Lake helpers: atomic file writes and the append-only run log."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl


def now_utc() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def write_parquet_atomic(df: pl.DataFrame, path: Path) -> None:
    """Write to a temp file in the same directory, then rename: readers never see a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    df.write_parquet(tmp, compression="zstd", statistics=True)
    os.replace(tmp, path)


def write_text_atomic(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def log_run(lake: Path, step: str, **fields) -> dict:
    rec = {"step": step, "at_utc": now_utc().isoformat(timespec="seconds"), **fields}
    lake.mkdir(parents=True, exist_ok=True)
    with (lake / "_runs.jsonl").open("a") as f:
        f.write(json.dumps(rec, default=str) + "\n")
    return rec


def read_runs(lake: Path) -> list[dict]:
    p = lake / "_runs.jsonl"
    return [json.loads(line) for line in p.read_text().splitlines()] if p.exists() else []
