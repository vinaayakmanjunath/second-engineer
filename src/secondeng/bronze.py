"""Bronze: land every new file as received, with a sha256 manifest. Idempotent."""

from __future__ import annotations

import gzip
import hashlib
import json
import shutil
from pathlib import Path

import polars as pl

from secondeng import lake
from secondeng.settings import Settings
from secondeng.simulator import landing_files

MANIFEST_SCHEMA = {
    "file_name": pl.String,
    "sha256": pl.String,
    "vessel_id": pl.String,
    "batch_id": pl.String,
    "logger": pl.String,
    "sent_at": pl.Datetime("us"),
    "received_at": pl.Datetime("us"),
    "n_lines": pl.Int64,
    "status": pl.String,
    "bronze_path": pl.String,
    "silvered_at": pl.Datetime("us"),
}


def manifest_path(s: Settings) -> Path:
    return s.lake / "bronze" / "_manifest.parquet"


def read_manifest(s: Settings) -> pl.DataFrame:
    p = manifest_path(s)
    return pl.read_parquet(p) if p.exists() else pl.DataFrame(schema=MANIFEST_SCHEMA)


def _header(raw: bytes) -> tuple[dict, int]:
    text = gzip.decompress(raw)
    first = json.loads(text[: text.index(b"\n")])
    return first, text.count(b"\n")


def ingest(s: Settings) -> dict:
    man = read_manifest(s)
    seen_files = set(man["file_name"])
    seen_sha = set(man["sha256"])
    received = lake.now_utc().replace(microsecond=0)
    new_rows = []
    for f in landing_files(s.landing):
        if f.name in seen_files:
            continue
        raw = f.read_bytes()
        sha = hashlib.sha256(raw).hexdigest()
        head, n_lines = _header(raw)
        row = {
            "file_name": f.name,
            "sha256": sha,
            "vessel_id": head["vessel_id"],
            "batch_id": head["batch_id"],
            "logger": head["logger"],
            "sent_at": head["sent_at"],
            "received_at": received,
            "n_lines": n_lines,
            "silvered_at": None,
        }
        if sha in seen_sha:
            row |= {"status": "duplicate_file", "bronze_path": None}
        else:
            dest = s.lake / "bronze" / f"vessel_id={head['vessel_id']}" / f"received_date={received:%Y-%m-%d}" / f.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_name("." + dest.name)
            shutil.copyfile(f, tmp)
            tmp.rename(dest)
            row |= {"status": "loaded", "bronze_path": str(dest.relative_to(s.lake))}
            seen_sha.add(sha)
        seen_files.add(f.name)
        new_rows.append(row)
    if new_rows:
        new = (
            pl.DataFrame(new_rows)
            .with_columns(pl.col("sent_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ", time_unit="us"))
            .select(list(MANIFEST_SCHEMA))
            .cast(MANIFEST_SCHEMA)
        )
        man = pl.concat([man, new])
        lake.write_parquet_atomic(man, manifest_path(s))
    counts = dict(pl.DataFrame(new_rows).group_by("status").len().iter_rows()) if new_rows else {}
    return lake.log_run(s.lake, "bronze", new_files=len(new_rows), **{f"files_{k}": v for k, v in counts.items()})
