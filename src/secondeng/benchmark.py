"""Benchmarks -> docs/benchmarks.md (generated). Runs in scratch dirs under data/bench/.

1. full build from the landed files minus one held-back batch
2. the held-back batch arrives late: incremental bronze/silver/gold, partitions rewritten
3. re-run with nothing new: idempotency (identical gold checksums, no partition rewritten)
4. storage sizes, throughput, and query latency (silver raw vs 1-minute rollup)
"""

from __future__ import annotations

import os
import platform
import resource
import shutil
import statistics
import subprocess
import time
from collections import Counter
from pathlib import Path

import duckdb

from secondeng import bronze, lake, silver
from secondeng.cli import gold
from secondeng.fleet import GLORIA_DIR
from secondeng.settings import ROOT, Settings
from secondeng.simulator import landing_files

CHECKSUM_SQL = {
    "mart_rollup_1min": "select count(*), round(sum(mean_value), 3) from mart_rollup_1min",
    "mart_alerts": "select count(*), string_agg(alert_id, ',' order by alert_id) from mart_alerts",
    "mart_dq_daily": "select count(*), sum(points_valid), sum(points_quarantined) from mart_dq_daily",
}


def _size(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


def _machine() -> str:
    mem = ""
    try:
        if platform.system() == "Darwin":
            mem = f", {int(subprocess.check_output(['sysctl', '-n', 'hw.memsize'])) / 2**30:.0f} GB RAM"
            chip = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
        else:
            chip = platform.processor() or platform.machine()
            kb = next(
                int(x.split()[1]) for x in Path("/proc/meminfo").read_text().splitlines() if x.startswith("MemTotal")
            )
            mem = f", {kb / 2**20:.0f} GB RAM"
    except (OSError, subprocess.CalledProcessError, StopIteration):
        chip = platform.machine()
    return f"{chip}, {os.cpu_count()} CPUs{mem}, {platform.system()} {platform.release()}, Python {platform.python_version()}"


def _checksums(s: Settings) -> dict:
    con = duckdb.connect(str(s.path(s.warehouse)), read_only=True)
    out = {k: con.execute(q).fetchall()[0] for k, q in CHECKSUM_SQL.items()}
    con.close()
    return out


def _timed(fn) -> tuple[float, object]:
    t0 = time.perf_counter()
    r = fn()
    return round(time.perf_counter() - t0, 2), r


def _median_ms(con, sql: str, n: int = 5) -> float:
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        con.sql(sql).fetchall()
        times.append((time.perf_counter() - t0) * 1000)
    return round(statistics.median(times), 1)


def write(main: Settings) -> Path:
    scratch = ROOT / "data" / "bench"
    shutil.rmtree(scratch, ignore_errors=True)
    s = main.model_copy(
        update={
            "landing_root": scratch / "landing",
            "lake_root": scratch / "lake",
            "warehouse": scratch / "lake" / "wh.duckdb",
        }
    )
    files = landing_files(main.landing)
    if not files:
        raise SystemExit("run the pipeline first (landing/ is empty)")
    # hold back one batch that was sent exactly once (a re-sent batch would just be a duplicate)
    batch_of = {f: f.name.split("_", 1)[1] for f in files}
    sent = Counter(batch_of.values())
    candidates = [f for f in files if f.parent.name == main.fleet.vessels[0].vessel_id and sent[batch_of[f]] == 1]
    held = candidates[len(candidates) // 2]
    for f in files:
        if f != held:
            (s.landing / f.parent.name).mkdir(parents=True, exist_ok=True)
            os.link(f, s.landing / f.parent.name / f.name)
    shutil.copy(main.landing / "_sim_log.json", s.landing / "_sim_log.json")

    full = {}
    full["bronze"], _ = _timed(lambda: bronze.ingest(s))
    full["silver"], sv = _timed(lambda: silver.run(s))
    # peak resident memory of this process so far = the full bronze + silver build (ru_maxrss: bytes on macOS)
    peak_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (2**20 if platform.system() == "Darwin" else 2**10)
    full["gold"], _ = _timed(lambda: gold(s, full_refresh=True))

    os.link(held, s.landing / held.parent.name / held.name)
    inc = {}
    inc["bronze"], _ = _timed(lambda: bronze.ingest(s))
    inc["silver"], sv_inc = _timed(lambda: silver.run(s))
    inc["gold"], _ = _timed(lambda: gold(s))
    before = _checksums(s)
    parts_before = {p: p.stat().st_mtime_ns for p in (s.lake / "silver" / "points").rglob("*.parquet")}

    rerun = {}
    rerun["bronze"], b_re = _timed(lambda: bronze.ingest(s))
    rerun["silver"], sv_re = _timed(lambda: silver.run(s))
    rerun["gold"], _ = _timed(lambda: gold(s))
    after = _checksums(s)
    parts_after = {p: p.stat().st_mtime_ns for p in (s.lake / "silver" / "points").rglob("*.parquet")}

    raw_csv = _size(main.raw / GLORIA_DIR)
    landing_b = _size(main.landing)
    bronze_b = _size(s.lake / "bronze")
    silver_b = _size(s.lake / "silver" / "points")
    con = duckdb.connect(str(s.path(s.warehouse)), read_only=True)
    rollup_rows = con.execute("select count(*) from mart_rollup_1min").fetchall()[0][0]
    vessel = main.injection.vessel_id
    week = con.execute(
        f"select min(minute), min(minute) + interval 7 day from mart_rollup_1min where vessel_id = '{vessel}'"
    ).fetchall()[0]
    pts = f"read_parquet('{s.lake}/silver/points/*/*/*.parquet', hive_partitioning = true)"
    q_raw = (
        f"select date_trunc('hour', ts_utc) h, avg(value) from {pts} where vessel_id = '{vessel}' "
        f"and sensor_id = 'me.exh_temp_cyl3_c' and ts_utc >= '{week[0]}' and ts_utc < '{week[1]}' group by 1"
    )
    q_roll = (
        f"select date_trunc('hour', minute) h, sum(mean_value * n) / sum(n) from mart_rollup_1min "
        f"where vessel_id = '{vessel}' and sensor_id = 'me.exh_temp_cyl3_c' and minute >= '{week[0]}' "
        f"and minute < '{week[1]}' group by 1"
    )
    lat_raw, lat_roll = _median_ms(con, q_raw), _median_ms(con, q_roll)
    con.close()

    mb = lambda b: f"{b / 2**20:,.1f} MB"  # noqa: E731
    out = [
        "# Benchmarks (generated)",
        "",
        f"_Generated {lake.now_utc():%Y-%m-%d %H:%M} UTC by `secondeng bench` on: {_machine()}._",
        "",
        "## Storage",
        "",
        "| layer | size | note |",
        "|---|---:|---|",
        f"| raw Gloria CSV (3 files) | {mb(raw_csv)} | as published |",
        f"| landing (gzip JSON lines, satellite batches) | {mb(landing_b)} | {len(files):,} files incl. re-sends |",
        f"| bronze (as received) | {mb(bronze_b)} | identical re-sends not stored |",
        f"| silver points (Parquet zstd, long format) | {mb(silver_b)} | {sv['points_valid'] + sv_inc['points_valid']:,} readings |",
        f"| gold 1-minute rollup | {rollup_rows:,} rows | in DuckDB |",
        "",
        f"Silver Parquet is **{raw_csv / silver_b:.1f}x smaller** than the raw CSV.",
        "",
        "## Full build vs one late batch",
        "",
        "| step | full build (s) | one late batch (s) | re-run, nothing new (s) |",
        "|---|---:|---:|---:|",
    ]
    out += [f"| {k} | {full[k]} | {inc[k]} | {rerun[k]} |" for k in full]
    out += [
        f"| **total** | **{sum(full.values()):.1f}** | **{sum(inc.values()):.1f}** | **{sum(rerun.values()):.1f}** |",
        "",
        f"- Full build: silver parsed {sv['points_valid'] + sv['points_quarantined']:,} readings in {sv['seconds']} s "
        f"(**{(sv['points_valid'] + sv['points_quarantined']) / sv['seconds']:,.0f} readings/s**), "
        f"{sv['partitions_rewritten']} vessel-day partitions written.",
        f"- Peak memory of the full bronze + silver build: **{peak_mib / 1024:.1f} GiB** (resident, one process; silver "
        f"parses {silver.CHUNK_FILES} files per chunk).",
        f"- Late batch `{held.name}`: silver rewrote **{sv_inc['partitions_rewritten']} partition(s)** "
        f"({', '.join(sv_inc['partitions'])}); dbt rollup is incremental on the touched vessel-days.",
        "",
        "## Idempotency (re-run with nothing new)",
        "",
        f"- bronze new files: {b_re['new_files']}; silver files processed: {sv_re['files']}; "
        f"partitions rewritten: {sv_re['partitions_rewritten']}",
        f"- silver partition files unchanged: **{parts_before == parts_after}**",
        f"- gold checksums identical: **{before == after}** ({', '.join(CHECKSUM_SQL)})",
        "",
        "## Query latency (median of 5)",
        "",
        f"One week of hourly `me.exh_temp_cyl3_c` for {vessel}:",
        "",
        f"- from silver raw Parquet (1 Hz): **{lat_raw} ms**",
        f"- from the gold 1-minute rollup: **{lat_roll} ms**",
        "",
    ]
    path = main.path(Path("docs/benchmarks.md"))
    lake.write_text_atomic("\n".join(out), path)
    return path
