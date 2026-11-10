"""Shared fixtures: real config, every path redirected into a temp dir. No downloaded data needed."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from secondeng import settings


@pytest.fixture
def s(tmp_path: Path):
    base = settings.load()
    return base.model_copy(
        update={
            "lake_root": tmp_path / "lake",
            "landing_root": tmp_path / "landing",
            "raw_root": tmp_path / "raw",
            "warehouse": tmp_path / "wh.duckdb",
            "ground_truth": tmp_path / "gt.json",
            "site_data": tmp_path / "site",
        }
    )


def line(vessel, batch_id, sent_at, seq, source, ts, tags, values, logger="gloria_v1"):
    return {
        "vessel_id": vessel,
        "batch_id": batch_id,
        "logger": logger,
        "sent_at": sent_at,
        "seq": seq,
        "source": source,
        "ts": ts,
        "tags": tags,
        "values": values,
    }


def write_batch(landing: Path, name: str, lines: list[dict]) -> Path:
    vessel = lines[0]["vessel_id"]
    p = landing / vessel / name
    p.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(json.dumps(x) + "\n" for x in lines).encode()
    p.write_bytes(gzip.compress(body, mtime=0))
    return p
