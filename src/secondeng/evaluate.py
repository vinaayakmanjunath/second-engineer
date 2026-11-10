"""Evaluate the run against ground truth -> docs/results.md + docs/results.json.

The only module that reads data/ground_truth. Everything reported is measured from the
warehouse, the lake and the simulator log of this run.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import polars as pl

from secondeng import bronze, lake, silver
from secondeng.cli import build
from secondeng.settings import ROOT, Settings


def _utc(local_iso: str, tz: str) -> datetime:
    return (
        datetime.fromisoformat(local_iso).replace(tzinfo=ZoneInfo(tz)).astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    )


def ground_truth(s: Settings) -> dict:
    gt = json.loads(s.path(s.ground_truth).read_text())[0]
    return gt | {
        "ramp_start_utc": _utc(gt["ramp_start_local"], gt["timezone"]),
        "failure_utc": _utc(gt["failure_local"], gt["timezone"]),
    }


def isolated_run(main: Settings, severity: float, d: Path) -> Settings:
    """Rebuild only the injected vessel at a given fault severity, in its own dirs under d."""
    shutil.rmtree(d, ignore_errors=True)
    s = main.model_copy(
        update={
            "landing_root": d / "landing",
            "lake_root": d / "lake",
            "warehouse": d / "lake" / "wh.duckdb",
            "ground_truth": d / "ground_truth.json",
            "fleet": main.fleet.model_copy(update={"vessels": [main.vessel(main.injection.vessel_id)]}),
            "injection": main.injection.model_copy(update={"severity_x_bench": severity}),
        }
    )
    build(s)
    shutil.rmtree(d / "landing", ignore_errors=True)
    return s


def window_metrics(s: Settings) -> dict:
    """What the detector did on the injected vessel/system during the ramp window of this run."""
    gt = ground_truth(s)
    con = duckdb.connect(str(s.path(s.warehouse)), read_only=True)
    p = {"v": gt["vessel_id"], "sys": gt["system"], "start": gt["ramp_start_utc"], "end": gt["failure_utc"]}
    opened = con.execute(
        """select count(*), min(opened_at) from mart_alerts
           where vessel_id = $v and system = $sys and opened_at >= $start and opened_at < $end""",
        p,
    ).fetchone()
    hours = con.execute(
        f"""select count(*) filter (where n_sensors_flagged >= {s.detection.min_sensors}), max(max_abs_z)
            from mart_system_health where vessel_id = $v and system = $sys and hour >= $start and hour < $end""",
        p,
    ).fetchone()
    before = con.execute(
        "select count(*) from mart_alerts where vessel_id = $v and system = $sys and opened_at < $start",
        {k: p[k] for k in ("v", "sys", "start")},
    ).fetchone()[0]
    strongest = con.execute(
        """select alert_id from mart_alerts
           where vessel_id = $v and system = $sys and first_flagged < $end and last_seen > $start
           order by max_abs_z desc limit 1""",
        p,
    ).fetchone()
    matches = (
        con.execute(
            "select fault, rank from mart_fault_match where alert_id = ? order by rank", [strongest[0]]
        ).fetchall()
        if strongest
        else []
    )
    con.close()
    first = opened[1]
    return {
        "severity_x_bench": s.injection.severity_x_bench,
        "alerts_opened_in_window": opened[0],
        "first_opened_utc": first,
        "hours_before_failure": round((gt["failure_utc"] - first).total_seconds() / 3600, 1) if first else None,
        "flagged_hours_in_window": hours[0],
        "peak_abs_z_in_window": round(hours[1], 2) if hours[1] is not None else None,
        "strongest_alert_id": strongest[0] if strongest else None,
        "top_fault": matches[0][0] if matches else None,
        "injected_fault_rank": next((r for f, r in matches if f == gt["fault"]), None),
        "alerts_before_ramp": before,
    }


def paired_hours(fault_wh: Path, control_wh: Path, gt: dict, min_sensors: int) -> dict:
    """Hour-by-hour comparison of flags in the ramp window, fault run vs control (same vessel, same hours),
    plus how often the control flags healthy hours before the ramp (the honest false-positive base rate)."""
    con = duckdb.connect()
    con.execute(f"attach '{fault_wh}' as f (read_only)")
    con.execute(f"attach '{control_wh}' as c (read_only)")
    p = {
        "v": gt["vessel_id"],
        "sys": gt["system"],
        "start": gt["ramp_start_utc"],
        "end": gt["failure_utc"],
        "k": min_sensors,
    }
    both, fault_only, control_only, neither = con.execute(
        """select count(*) filter (where f.n_sensors_flagged >= $k and c.n_sensors_flagged >= $k),
                  count(*) filter (where f.n_sensors_flagged >= $k and c.n_sensors_flagged < $k),
                  count(*) filter (where f.n_sensors_flagged < $k and c.n_sensors_flagged >= $k),
                  count(*) filter (where f.n_sensors_flagged < $k and c.n_sensors_flagged < $k)
           from f.mart_system_health f join c.mart_system_health c using (vessel_id, system, hour)
           where vessel_id = $v and system = $sys and hour >= $start and hour < $end""",
        p,
    ).fetchone()
    pre = con.execute(
        """select avg((n_sensors_flagged >= $k)::int) from c.mart_system_health
           where vessel_id = $v and system = $sys and hour < $start""",
        {k: p[k] for k in ("v", "sys", "start", "k")},
    ).fetchone()[0]
    con.close()
    return {
        "both": both,
        "fault_only": fault_only,
        "control_only": control_only,
        "neither": neither,
        "control_pre_ramp_flag_rate": round(pre, 3) if pre is not None else None,
    }


def fault_vs_control(s: Settings, cs: Settings) -> dict:
    """Same vessel and detector with and without the fault. Lead time is claimed only if the control is quiet.

    Transport noise is seeded per vessel id, so the control is exactly paired with the full run."""
    fault = window_metrics(s)
    control = window_metrics(cs)
    claimed = control["alerts_opened_in_window"] == 0 and fault["alerts_opened_in_window"] > 0
    if claimed:
        verdict = f"Lead time attributable to the fault: {fault['hours_before_failure']} h."
    elif control["alerts_opened_in_window"]:
        verdict = "No lead time claimed: the no-fault control also opens an alert in the ramp window."
    else:
        verdict = "Not detected before the declared failure."
    return {
        "with_fault": fault,
        "control": control,
        "paired_hours": paired_hours(
            s.path(s.warehouse), cs.path(cs.warehouse), ground_truth(s), s.detection.min_sensors
        ),
        "attributable_lead_time_hours": fault["hours_before_failure"] if claimed else None,
        "verdict": verdict,
    }


FALSE_ALERTS_SQL = """
    with days as (
        select vessel_id, count(distinct cast(hour as date)) as running_days
        from mart_system_health group by all),
    fa as (select vessel_id, count(*) as alerts from mart_alerts group by all)
    select d.vessel_id, coalesce(fa.alerts, 0) as alerts, d.running_days,
           round(coalesce(fa.alerts, 0) / d.running_days, 3) as per_running_day
    from days d left join fa using (vessel_id) order by 1"""


def false_alerts(main_wh: Path, control_wh: Path, injected_vessel: str) -> list[dict]:
    """Alerts per day with running data, on data with no fault: the healthy vessels from the main run,
    the injected vessel from its no-fault control (where every alert is false by construction)."""
    rows = []
    for wh, keep in ((main_wh, lambda v: v != injected_vessel), (control_wh, lambda v: v == injected_vessel)):
        con = duckdb.connect(str(wh), read_only=True)
        cur = con.execute(FALSE_ALERTS_SQL)
        cols = [d[0] for d in cur.description]
        rows += [r for r in (dict(zip(cols, x, strict=True)) for x in cur.fetchall()) if keep(r["vessel_id"])]
        con.close()
    return sorted(rows, key=lambda r: r["vessel_id"])


def transport(s: Settings) -> list[dict]:
    """What the simulator injected vs what the pipeline caught, per vessel."""
    sim = json.loads((s.landing / "_sim_log.json").read_text())["vessels"]
    man = bronze.read_manifest(s)
    batches = silver.read_log(s, "batches")
    q = silver.read_log(s, "quarantine")
    valid = (
        pl.scan_parquet(s.lake / "silver" / "points" / "**" / "*.parquet", hive_partitioning=True)
        .group_by("vessel_id")
        .agg(pl.len().alias("n"))
        .collect()
    )
    rows = []
    for v, st in sim.items():
        qv = q.filter(pl.col("vessel_id") == v)

        def qn(reason: str, qv: pl.DataFrame = qv) -> int:
            return int(qv.filter(pl.col("reason") == reason)["n_rows"].sum())

        bv = batches.filter(pl.col("vessel_id") == v)
        n_valid = int(valid.filter(pl.col("vessel_id") == v)["n"].sum())
        checks = [
            (
                "identical re-sends dropped (sha256, bronze)",
                st.get("files_resend_identical", 0),
                man.filter((pl.col("vessel_id") == v) & (pl.col("status") == "duplicate_file")).height,
            ),
            (
                "re-sends with new envelope dropped (batch_id, silver)",
                st.get("files_resend_new_envelope", 0),
                bv.filter(pl.col("status") == "duplicate_batch").height,
            ),
            ("blank timestamps quarantined (readings)", st.get("corrupt_ts_blank_readings", 0), qn("bad_timestamp")),
            (
                "future timestamps quarantined (readings)",
                st.get("corrupt_ts_future_readings", 0),
                qn("future_timestamp"),
            ),
            ("readings accounted for (valid + quarantined = sent)", st["readings"], n_valid + int(qv["n_rows"].sum())),
        ]
        rows += [
            {"vessel_id": v, "check": c, "injected": int(i), "handled": int(h), "match": int(i) == int(h)}
            for c, i, h in checks
        ]
        rows.append(
            {
                "vessel_id": v,
                "check": "late or outage-delayed batches ingested (>1 h after data)",
                "injected": int(st.get("late_batches", 0) + st.get("outage_batches", 0)),
                "handled": bv.filter(
                    (pl.col("status") == "loaded") & ((pl.col("sent_at") - pl.col("max_ts_utc")).dt.total_hours() >= 1)
                ).height,
                "match": None,
            }
        )
        rows.append(
            {
                "vessel_id": v,
                "check": "rows shuffled in transit / order breaks seen on arrival",
                "injected": int(st.get("ooo_rows", 0)),
                "handled": int(bv["out_of_order_rows"].sum()),
                "match": None,
            }
        )
    return rows


def write(s: Settings) -> dict:
    gt = ground_truth(s)
    cs = isolated_run(s, 0.0, ROOT / "data" / "control")  # same vessel, no fault
    fvc = fault_vs_control(s, cs)
    res = {
        "generated_utc": lake.now_utc().isoformat(timespec="seconds") + "Z",
        "injection": {
            k: gt[k]
            for k in ("vessel_id", "system", "fault", "severity_x_bench", "ramp_hours", "ramp_start_utc", "failure_utc")
        },
        "fault_vs_control": fvc,
        "min_sensors": s.detection.min_sensors,
        "false_alerts": false_alerts(s.path(s.warehouse), cs.path(cs.warehouse), gt["vessel_id"]),
        "transport": transport(s),
    }
    docs = s.path(Path("docs"))
    docs.mkdir(exist_ok=True)
    lake.write_text_atomic(json.dumps(res, indent=2, default=str), docs / "results.json")
    lake.write_text_atomic(render(res), docs / "results.md")
    return res


METRIC_LABELS = [
    ("alerts_opened_in_window", "alerts opened"),
    ("hours_before_failure", "first alert, hours before failure"),
    ("flagged_hours_in_window", "flagged hours (>= min sensors at abs z >= threshold)"),
    ("peak_abs_z_in_window", "peak max abs z"),
    ("top_fault", "strongest alert: top fault match"),
    ("injected_fault_rank", "strongest alert: rank of the injected fault"),
    ("alerts_before_ramp", "alerts before the ramp (false alarms, identical by construction)"),
]


def render(r: dict) -> str:
    inj, fvc = r["injection"], r["fault_vs_control"]
    out = [
        "# Results (generated)",
        "",
        f"_Generated {r['generated_utc']} by `secondeng evaluate`. Do not edit by hand._",
        "",
        "## Injected fault (ground truth, never read by the pipeline)",
        "",
        f"- Vessel / system: **{inj['vessel_id']} / {inj['system']}**",
        f"- Fault: **{inj['fault']}** (bench signature x {inj['severity_x_bench']})",
        f"- Ramp: {inj['ramp_hours']} h from {inj['ramp_start_utc']} UTC to declared failure {inj['failure_utc']} UTC",
        "",
        "## Fault vs no-fault control (same vessel, same detector)",
        "",
        f"**{fvc['verdict']}**",
        "",
        "| measure (ramp window) | with fault | control (no fault) |",
        "|---|---:|---:|",
    ]
    for key, name in METRIC_LABELS:
        a, b = fvc["with_fault"][key], fvc["control"][key]
        out.append(f"| {name} | {'-' if a is None else a} | {'-' if b is None else b} |")
    ph = fvc["paired_hours"]
    out += [
        "",
        "Hour by hour in the ramp window (same vessel, same hours; flagged = at least "
        f"{r['min_sensors']} sensors at abs z >= threshold):",
        "",
        "| flagged in fault run only | in control only | in both | in neither |",
        "|---:|---:|---:|---:|",
        f"| {ph['fault_only']} | {ph['control_only']} | {ph['both']} | {ph['neither']} |",
        "",
        f"Before the ramp the control flags {ph['control_pre_ramp_flag_rate']:.0%} of healthy hours: "
        "natural drift, the reason no lead time is claimed.",
        "",
        "## False alerts (alerts on data with no fault)",
        "",
        "Healthy vessels from this run; the injected vessel from its no-fault control, where every alert is false.",
        "",
        "| vessel | alerts | days with running data | per running day |",
        "|---|---:|---:|---:|",
    ]
    out += [
        f"| {x['vessel_id']} | {x['alerts']} | {x['running_days']} | {x['per_running_day']} |"
        for x in r["false_alerts"]
    ]
    out += [
        "",
        "## Transport mess: injected vs handled",
        "",
        "| vessel | check | injected | handled | exact match |",
        "|---|---|---:|---:|:---:|",
    ]
    out += [
        f"| {x['vessel_id']} | {x['check']} | {x['injected']:,} | {x['handled']:,} | "
        f"{'yes' if x['match'] else ('no' if x['match'] is False else 'n/a')} |"
        for x in r["transport"]
    ]
    out += [
        "",
        "Late/out-of-order rows are informational: delays overlap with outages, and a shuffled row "
        "may land in its original position.",
        "",
    ]
    return "\n".join(out)
