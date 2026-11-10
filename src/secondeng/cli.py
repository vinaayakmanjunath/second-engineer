"""`secondeng` command line: one subcommand per pipeline step, `run` does them all."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import polars as pl

from secondeng import lake, settings
from secondeng.settings import ROOT, Settings

DBT_DIR = ROOT / "dbt"


def write_seeds(s: Settings) -> None:
    seeds = DBT_DIR / "seeds"
    seeds.mkdir(exist_ok=True)
    pl.DataFrame(
        [
            {
                "sensor_id": x.id,
                "system": x.system,
                "measurement": x.measurement,
                "unit": x.unit,
                "min_value": x.min,
                "max_value": x.max,
            }
            for x in s.registry
        ]
    ).write_csv(seeds / "sensor_registry.csv")
    pl.DataFrame([{"vessel_id": v.vessel_id, "owner": v.owner, "logger": v.logger} for v in s.fleet.vessels]).write_csv(
        seeds / "fleet.csv"
    )
    if not (seeds / "fault_signatures.csv").exists():
        from secondeng import signatures

        signatures.write(s)


def dbt_vars(s: Settings) -> dict:
    return s.detection.model_dump() | {
        "event_usd": s.cost_model.event_usd,
        "severity_weight": s.cost_model.severity_weight,
    }


def gold(s: Settings, full_refresh: bool = False) -> dict:
    write_seeds(s)
    env = os.environ | {
        "SECONDENG_LAKE": str(s.lake),
        "SECONDENG_WAREHOUSE": str(s.path(s.warehouse)),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    dbt = Path(sys.executable).parent / "dbt"
    cmd = [
        str(dbt),
        "build",
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
        "--vars",
        json.dumps(dbt_vars(s)),
    ]
    if full_refresh:
        cmd.append("--full-refresh")
    t0 = time.perf_counter()
    subprocess.run(cmd, env=env, check=True)
    return lake.log_run(s.lake, "gold", seconds=round(time.perf_counter() - t0, 2), full_refresh=full_refresh)


def build(s: Settings, simulate: bool = True) -> dict[str, float]:
    """simulate -> bronze -> silver -> gold for one settings object; returns seconds per step."""
    from secondeng import bronze, silver, simulator

    t: dict[str, float] = {}
    steps = [("simulate", lambda: simulator.simulate(s))] if simulate else []
    steps += [("bronze", lambda: bronze.ingest(s)), ("silver", lambda: silver.run(s)), ("gold", lambda: gold(s))]
    for name, fn in steps:
        t0 = time.perf_counter()
        fn()
        t[name] = round(time.perf_counter() - t0, 2)
    return t


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="secondeng")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("download", help="fetch both Zenodo datasets (md5-verified)")
    sub.add_parser("profile", help="write docs/data_profile.md")
    sim = sub.add_parser("simulate", help="replay the fleet as satellite batches into landing/")
    sim.add_argument("--until", help="only send batches with sent_at <= this UTC time (ISO)")
    sub.add_parser("ingest", help="landing -> bronze (+ manifest)")
    sub.add_parser("silver", help="bronze -> silver (incremental partitions)")
    g = sub.add_parser("gold", help="dbt build (gold marts + tests)")
    g.add_argument("--full-refresh", action="store_true")
    sub.add_parser("evaluate", help="write docs/results.md")
    sub.add_parser("export", help="gold -> site/data for the dashboard")
    sub.add_parser("bench", help="write docs/benchmarks.md (uses data/bench scratch dirs)")
    sub.add_parser("sensitivity", help="write docs/sensitivity.md: lead time vs fault severity (data/sensitivity)")
    sub.add_parser("run", help="download, simulate, ingest, silver, gold, evaluate, export")
    api = sub.add_parser("api", help="serve the owner-scoped API")
    api.add_argument("--port", type=int, default=8000)
    a = p.parse_args(argv)
    s = settings.load()

    if a.cmd in ("download", "run"):
        from secondeng.download import download

        download(s.raw)
    if a.cmd == "profile":
        from secondeng import profile

        profile.write(s)
    if a.cmd in ("simulate", "run"):
        from secondeng import simulator

        until = datetime.fromisoformat(a.until) if getattr(a, "until", None) else None
        print(json.dumps(simulator.simulate(s, until)["vessels"], indent=1))
    if a.cmd in ("ingest", "run"):
        from secondeng import bronze

        print(bronze.ingest(s))
    if a.cmd in ("silver", "run"):
        from secondeng import silver

        r = silver.run(s)
        print({k: v for k, v in r.items() if k != "partitions"})
    if a.cmd in ("gold", "run"):
        print(gold(s, getattr(a, "full_refresh", False)))
    if a.cmd in ("evaluate", "run"):
        from secondeng import evaluate

        evaluate.write(s)
    if a.cmd in ("export", "run"):
        from secondeng import export

        export.write(s)
    if a.cmd == "bench":
        from secondeng import benchmark

        benchmark.write(s)
    if a.cmd == "sensitivity":
        from secondeng import sensitivity

        sensitivity.write(s)
    if a.cmd == "api":
        import uvicorn

        uvicorn.run("secondeng.api:app", host="0.0.0.0", port=a.port)


if __name__ == "__main__":
    main()
