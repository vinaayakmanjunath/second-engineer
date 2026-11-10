"""Fault signature library from the marine-engine test bench.

For every bench fault file with a pre-anomaly segment, and every canonical measurement the
bench and the fleet both have, signature = (mean(anomaly) - mean(pre)) / |mean(pre)|.
Measurement value = mean of that measurement's sensors (e.g. exhaust temp over cylinders).
Injector-nozzle files have no pre-anomaly segment and are left out (documented limitation).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from secondeng.settings import Settings

BENCH_DIR = "bench/Marine_Engine_Fault_Data_v1/Marine_Engine_Fault_Data"
FAULTS = {
    "AC_Fouling": "air_cooler_fouling",
    "AF_Clogging": "air_filter_clogging",
    "Pump_Cavitation": "cw_pump_cavitation",
    "Turbine_Degradation": "turbine_degradation",
}


def read_bench_csv(path: Path) -> pl.DataFrame:
    """3-row header: names, symbols, units. Data from row 4."""
    names = pl.read_csv(path, n_rows=0).columns
    return pl.read_csv(path, skip_rows=3, has_header=False, new_columns=names, infer_schema_length=10000)


def fleet_measurements(s: Settings) -> list[str]:
    """Measurements the bench and the fleet loggers both report (load proxy 'speed' excluded)."""

    def meas(logger: str) -> set[str]:
        return {s.sensors[t.sensor].measurement for tags in s.loggers[logger].sources.values() for t in tags.values()}

    fleet = set.intersection(*(meas(v.logger) for v in s.fleet.vessels))
    return sorted((fleet & meas("bench")) - {"speed"})


def to_measurements(df: pl.DataFrame, s: Settings) -> pl.DataFrame:
    """Map bench columns to canonical sensors (scale/offset) and average per measurement."""
    tags = s.loggers["bench"].sources["test_bench"]
    keep = fleet_measurements(s)
    by_meas: dict[str, list[pl.Expr]] = {}
    for col, t in tags.items():
        m = s.sensors[t.sensor].measurement
        if m in keep:
            by_meas.setdefault(m, []).append(pl.col(col).cast(pl.Float64) * t.scale + t.offset)
    return df.select(
        pl.col("Anomaly State").cast(pl.Int8).alias("anomaly"),
        *[pl.mean_horizontal(exprs).alias(m) for m, exprs in by_meas.items()],
    )


def build(s: Settings) -> pl.DataFrame:
    root = s.raw / BENCH_DIR
    rows = []
    for folder, fault in FAULTS.items():
        for f in sorted((root / folder).glob("*.csv")):
            load_pct = int(f.stem.split("_")[-2])
            m = to_measurements(read_bench_csv(f), s)
            pre, ano = m.filter(pl.col("anomaly") == 0), m.filter(pl.col("anomaly") == 1)
            for meas in m.columns[1:]:
                base = pre[meas].mean()
                rows.append(
                    {
                        "fault": fault,
                        "load_pct": load_pct,
                        "measurement": meas,
                        "pre_mean": base,
                        "anomaly_mean": ano[meas].mean(),
                        "rel_dev": (ano[meas].mean() - base) / abs(base),
                        "source_file": f.name,
                    }
                )
    return pl.DataFrame(rows)


def mean_signature(sig: pl.DataFrame) -> pl.DataFrame:
    """One vector per fault: mean relative deviation across tested loads."""
    return sig.group_by("fault", "measurement").agg(pl.col("rel_dev").mean()).sort("fault", "measurement")


def write(s: Settings) -> pl.DataFrame:
    sig = build(s)
    out = s.path(Path("dbt/seeds/fault_signatures.csv"))
    out.parent.mkdir(parents=True, exist_ok=True)
    mean_signature(sig).write_csv(out)
    detail = s.lake / "reference" / "fault_signatures_by_load.parquet"
    detail.parent.mkdir(parents=True, exist_ok=True)
    sig.write_parquet(detail)
    return sig
