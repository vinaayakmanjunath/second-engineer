"""Publish the gold tables the API serves from DuckDB into Postgres (schema `serving`), atomically.

DuckDB stays the batch engine and system of record; Postgres only serves. The copy goes into
`serving_next`; one transaction then drops `serving`, renames `serving_next` to `serving` and grants
the read-only API role. A run that fails or is cancelled before that transaction leaves the live
`serving` schema untouched; the next run drops the leftover `serving_next`.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import psycopg

# The serving contract: exactly these columns are published and nothing else is readable by the API.
SERVED: dict[str, list[str]] = {
    "fleet": ["vessel_id", "owner"],
    "sensor_registry": ["sensor_id", "system", "measurement", "unit"],
    "mart_fleet": [
        "vessel_id",
        "owner",
        "health_index",
        "open_alerts",
        "total_alerts",
        "usd_at_risk_open",
        "last_reading_utc",
        "freshness_lag_hours",
    ],
    "mart_system_health": ["vessel_id", "system", "hour", "health_index", "max_abs_z", "n_sensors_flagged"],
    "mart_alerts": [
        "alert_id",
        "vessel_id",
        "system",
        "first_flagged",
        "opened_at",
        "last_seen",
        "flagged_hours",
        "max_abs_z",
        "severity",
        "status",
        "sensors",
        "usd_at_risk",
    ],
    "mart_fault_match": ["alert_id", "fault", "similarity", "rank", "confident", "margin"],
    "mart_dq_daily": [
        "vessel_id",
        "day",
        "points_valid",
        "points_quarantined",
        "valid_pct",
        "coverage_pct",
        "batches_loaded",
        "duplicate_batches",
        "duplicate_files",
        "late_batches",
        "lateness_p95_min",
        "out_of_order_rows",
        "flatlined_sensor_hours",
    ],
    "mart_rollup_1min": ["vessel_id", "sensor_id", "minute", "min_value", "mean_value", "max_value", "n"],
}

KEYS = {
    "fleet": "primary key (vessel_id)",
    "sensor_registry": "primary key (sensor_id)",
    "mart_alerts": "primary key (alert_id)",
    "mart_fault_match": "primary key (alert_id, rank)",
    "mart_rollup_1min": "primary key (vessel_id, sensor_id, minute)",
    "mart_system_health": "primary key (vessel_id, system, hour)",
}
READER = "api_reader"


def publish(warehouse: Path, url: str) -> dict[str, int]:
    """Copy SERVED from the DuckDB warehouse into Postgres and swap it in. Returns rows per table.

    DuckDB's postgres extension does the bulk copy; psycopg owns the DDL and the swap transaction."""
    with psycopg.connect(url, autocommit=True) as pg:
        pg.execute("drop schema if exists serving_next cascade")
        pg.execute("create schema serving_next")
        counts = _copy(warehouse, url)
        for table, key in KEYS.items():
            pg.execute(f"alter table serving_next.{table} add {key}")
        pg.execute("analyze")
        # the swap: one transaction, so readers see either the old serving schema or the new one
        with pg.transaction():
            pg.execute("set local lock_timeout = '5s'")
            pg.execute("drop schema if exists serving cascade")
            pg.execute("alter schema serving_next rename to serving")
            pg.execute(f"grant usage on schema serving to {READER}")
            pg.execute(f"grant select on all tables in schema serving to {READER}")
    return counts


def _copy(warehouse: Path, url: str) -> dict[str, int]:
    con = duckdb.connect()  # in-memory session: the warehouse is attached read-only, Postgres writable
    try:
        con.execute("install postgres; load postgres")
        con.execute("attach ? as wh (read_only)", [str(warehouse)])
        con.execute("attach ? as pg (type postgres)", [url])
        counts = {}
        for table, cols in SERVED.items():
            select = ", ".join(f'"{c}"' for c in cols)
            con.execute(f"create table pg.serving_next.{table} as select {select} from wh.{table}")
            counts[table] = con.execute(f"select count(*) from wh.{table}").fetchone()[0]
        return counts
    finally:
        con.close()
