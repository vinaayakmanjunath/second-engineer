"""Profile both raw datasets -> docs/data_profile.md (generated)."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from secondeng import fleet, lake, signatures
from secondeng.settings import Settings


def _md(df: pl.DataFrame) -> str:
    cols = df.columns
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for row in df.iter_rows():
        lines.append(
            "| "
            + " | ".join("" if v is None else (f"{v:,.3g}" if isinstance(v, float) else str(v)) for v in row)
            + " |"
        )
    return "\n".join(lines)


def _columns(df: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for c in df.columns:
        if c == "timestamp" or not df[c].dtype.is_numeric():
            continue
        x = df[c]
        rows.append(
            {
                "column": c,
                "null_pct": round(100 * x.null_count() / len(x), 2),
                "zero_pct": round(100 * (x == 0).sum() / len(x), 2),
                "min": x.min(),
                "p1": x.quantile(0.01),
                "median": x.median(),
                "p99": x.quantile(0.99),
                "max": x.max(),
            }
        )
    return pl.DataFrame(rows)


def write(s: Settings) -> Path:
    out = [
        "# Data profile (generated)",
        "",
        f"_Generated {lake.now_utc():%Y-%m-%d %H:%M} UTC by `secondeng profile`._",
        "",
    ]
    out += ["## ARC Gloria (vendor A logger, 1 Hz, local time America/Bogota)", ""]
    for source in fleet.SOURCES:
        df = fleet.read_source(s, source).sort("timestamp")
        dt = df["timestamp"].diff().dt.total_seconds()
        span = (df["timestamp"].max() - df["timestamp"].min()).total_seconds() + 1
        gaps = (
            df.with_columns(dt.alias("gap_s"))
            .filter(pl.col("gap_s") > 6 * 3600)
            .sort("gap_s", descending=True)
            .head(10)
        )
        out += [
            f"### {source}",
            "",
            f"- rows: {df.height:,}; time range {df['timestamp'].min()} to {df['timestamp'].max()} (local)",
            f"- median sampling interval: {dt.median()} s; samples present vs 1 Hz over the span: {100 * df.height / span:.1f}%",
            f"- gaps > 6 h: {int((dt > 6 * 3600).sum())}; longest: {dt.max() / 3600:.1f} h",
            "- GPS / position channels: none",
            "",
        ]
        if gaps.height:
            out += [
                "Top gaps (end of gap, hours):",
                "",
                _md(
                    gaps.select(
                        pl.col("timestamp").alias("resumes_at"), (pl.col("gap_s") / 3600).round(1).alias("hours")
                    )
                ),
                "",
            ]
        out += [_md(_columns(df)), ""]
    legs = (
        fleet.leg_spans(s)
        .join(
            fleet.read_source(s, "main_engine").group_by("navigation_leg").agg(pl.len().alias("rows")),
            on="navigation_leg",
        )
        .sort("start")
    )
    out += ["### Voyage legs (main engine) and virtual-vessel assignment", ""]
    owner = {leg: v.vessel_id for v in s.fleet.vessels for leg in v.legs}
    out += [
        _md(legs.with_columns(pl.col("navigation_leg").replace_strict(owner, default="(unused)").alias("vessel"))),
        "",
    ]

    out += ["## Marine Engine Fault test bench (vendor B logger, 3-row header)", ""]
    root = s.raw / signatures.BENCH_DIR
    rows = []
    for f in sorted(root.glob("*/*.csv")):
        df = signatures.read_bench_csv(f)
        a = df["Anomaly State"]
        rows.append(
            {
                "file": f"{f.parent.name}/{f.name}",
                "rows": df.height,
                "columns": df.width,
                "pre_anomaly_rows": int((a == 0).sum()),
                "anomaly_rows": int((a == 1).sum()),
            }
        )
    ref = signatures.read_bench_csv(root / "Reference_Data.csv")
    out += [
        _md(pl.DataFrame(rows)),
        "",
        f"- Reference_Data.csv: {ref.height:,} rows, {ref.width} columns (no Anomaly State)",
        "",
    ]
    tags = s.loggers["bench"].sources["test_bench"]
    out += [
        "Bench channels mapped to the canonical model: " + ", ".join(f"`{k}` -> `{t.sensor}`" for k, t in tags.items()),
        "",
        f"Measurements shared by bench and fleet (used for fault signatures): {', '.join(signatures.fleet_measurements(s))}",
        "",
    ]
    path = s.path(Path("docs/data_profile.md"))
    path.parent.mkdir(exist_ok=True)
    lake.write_text_atomic("\n".join(out), path)
    return path
