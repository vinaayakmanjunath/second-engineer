"""Export small, dashboard-ready JSON files from gold to site/data/ (the static site reads these)."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import duckdb
import polars as pl

from secondeng import lake
from secondeng.settings import ROOT, Settings


def _records(df: pl.DataFrame) -> list[dict]:
    df = df.with_columns(
        [pl.col(c).dt.strftime("%Y-%m-%dT%H:%M:%SZ") for c, t in df.schema.items() if isinstance(t, pl.Datetime)]
        + [pl.col(c).cast(pl.String) for c, t in df.schema.items() if t == pl.Date]
        + [pl.col(c).cast(pl.Float64).round(4) for c, t in df.schema.items() if isinstance(t, pl.Decimal)]
    )
    return df.to_dicts()


def _git_sha() -> str | None:
    try:
        return (
            subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True
            ).stdout.strip()
            or None
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def write(s: Settings) -> dict:
    out = s.path(s.site_data)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(s.path(s.warehouse)), read_only=True)
    q = lambda sql: con.sql(sql).pl()  # noqa: E731

    files: dict[str, object] = {}
    files["fleet.json"] = _records(q("select * from mart_fleet order by vessel_id"))
    alerts = q("select * from mart_alerts order by vessel_id, first_flagged")
    matches = q("select * from mart_fault_match order by alert_id, rank")
    by_alert: dict[str, list] = {}
    for m in _records(matches):
        by_alert.setdefault(m.pop("alert_id"), []).append(m)
    files["alerts.json"] = [a | {"fault_match": by_alert.get(a["alert_id"], [])} for a in _records(alerts)]
    files["health.json"] = _records(
        q("""
        select vessel_id, system, hour, round(health_index, 1) as health, round(max_abs_z, 2) as max_abs_z,
               n_sensors_flagged as flagged
        from mart_system_health order by vessel_id, system, hour""")
    )
    files["dq.json"] = _records(q("select * from mart_dq_daily order by vessel_id, day"))
    files["sensors.json"] = _records(
        q("""
        select r.sensor_id, r.system, r.measurement, r.unit, r.min_value, r.max_value,
               exists(select 1 from mart_baselines b where b.sensor_id = r.sensor_id) as scored
        from sensor_registry r order by sensor_id""")
    )
    files["baselines.json"] = _records(
        q("""
        select vessel_id, sensor_id, speed_conditioned, round(intercept, 4) as intercept, round(slope, 6) as slope,
               round(sigma, 4) as sigma, round(rpm_min, 1) as rpm_min, round(rpm_max, 1) as rpm_max, n_hours,
               ref_start, ref_end
        from mart_baselines order by vessel_id, sensor_id""")
    )
    # one series file per vessel: hourly mean of every sensor while running, plus the model's expectation
    for (vessel_id,) in con.sql("select vessel_id from mart_fleet order by 1").fetchall():
        series = q(f"""
            with hourly as (
                select r.sensor_id, date_trunc('hour', r.minute) as hour,
                       sum(r.mean_value * r.n) / sum(r.n) as value
                from mart_rollup_1min r
                join int_operating_state o using (vessel_id, system, minute)
                where r.vessel_id = '{vessel_id}' and o.state = 'running'
                group by all having count(*) >= 30
            )
            select h.sensor_id, h.hour, round(h.value, 3) as value,
                   round(z.expected, 3) as expected, round(z.sigma, 4) as sigma, round(z.z, 2) as z
            from hourly h
            left join mart_anomaly_scores z
              on z.vessel_id = '{vessel_id}' and z.sensor_id = h.sensor_id and z.hour = h.hour
            order by h.sensor_id, h.hour""")
        cols: dict[str, dict[str, list]] = {}
        for sid, g in series.group_by("sensor_id", maintain_order=True):
            g = g.with_columns(pl.col("hour").dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
            cols[sid[0]] = {c: g[c].to_list() for c in ("hour", "value", "expected", "sigma", "z")}
        files[f"series_{vessel_id}.json"] = cols
    con.close()

    sim_log = s.landing / "_sim_log.json"
    results = s.path(Path("docs/results.json"))
    files["run.json"] = {
        "generated_utc": lake.now_utc().isoformat(timespec="seconds") + "Z",
        "git_sha": _git_sha(),
        "steps": [r for r in lake.read_runs(s.lake) if r["step"] in ("bronze", "silver", "gold")][-6:],
        "simulator": json.loads(sim_log.read_text())["vessels"] if sim_log.exists() else None,
        "results": json.loads(results.read_text()) if results.exists() else None,
        "settings": {
            "detection": s.detection.model_dump(),
            "injection": s.injection.model_dump(),
            "cost_event_usd": s.cost_model.event_usd,
            "simulator": s.simulator.model_dump(),
        },
    }
    sizes = {}
    for name, obj in files.items():
        lake.write_text_atomic(json.dumps(obj, separators=(",", ":"), default=str), out / name)
        sizes[name] = (out / name).stat().st_size
    return lake.log_run(s.lake, "export", files=len(sizes), total_kb=round(sum(sizes.values()) / 1024))
