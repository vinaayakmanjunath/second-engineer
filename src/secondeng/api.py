"""Owner-scoped read API over the gold warehouse.

Auth: header X-API-Key. Keys come from env API_KEYS='{"<key>": "<owner>", ...}' (never from the repo).
Unknown/missing key -> 401. A vessel owned by someone else -> 403. Unknown vessel -> 404.
"""

from __future__ import annotations

import hmac
import json
import os
from datetime import datetime, timedelta
from typing import Annotated, Literal

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException, Query

from secondeng import settings

app = FastAPI(
    title="Second Engineer API",
    version="0.1.0",
    description="Demonstration on open data: virtual vessels, injected fault, not validated.",
)


def _keys() -> dict[str, str]:
    raw = os.environ.get("API_KEYS", "{}")
    try:
        keys = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError("API_KEYS must be a JSON object of key -> owner") from e
    return {str(k): str(v) for k, v in keys.items()}


def _db() -> duckdb.DuckDBPyConnection:
    path = os.environ.get("SECONDENG_WAREHOUSE")
    if not path:
        s = settings.load()
        path = str(s.path(s.warehouse))
    return duckdb.connect(path, read_only=True)


def owner(x_api_key: Annotated[str | None, Header()] = None) -> str:
    if x_api_key:
        for key, who in _keys().items():
            if hmac.compare_digest(key.encode(), x_api_key.encode()):
                return who
    raise HTTPException(status_code=401, detail="missing or invalid API key")


Owner = Annotated[str, Depends(owner)]


def _rows(con: duckdb.DuckDBPyConnection, sql: str, params: list) -> list[dict]:
    cur = con.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def _authorise(con: duckdb.DuckDBPyConnection, vessel_id: str, who: str) -> None:
    row = con.execute("select owner from fleet where vessel_id = ?", [vessel_id]).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="unknown vessel")
    if row[0] != who:
        raise HTTPException(status_code=403, detail="vessel belongs to another owner")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/vessels")
def vessels(who: Owner) -> list[dict]:
    with _db() as con:
        return _rows(con, "select * from mart_fleet where owner = ? order by vessel_id", [who])


@app.get("/vessels/{vessel_id}/health")
def vessel_health(vessel_id: str, who: Owner, hours: int = Query(168, ge=1, le=24 * 90)) -> list[dict]:
    with _db() as con:
        _authorise(con, vessel_id, who)
        return _rows(
            con,
            """
            select system, hour, round(health_index, 1) as health_index, max_abs_z, n_sensors_flagged
            from mart_system_health
            where vessel_id = ? and hour >= (select max(hour) from mart_system_health where vessel_id = ?) - to_hours(?)
            order by system, hour""",
            [vessel_id, vessel_id, hours],
        )


@app.get("/vessels/{vessel_id}/alerts")
def vessel_alerts(vessel_id: str, who: Owner) -> list[dict]:
    with _db() as con:
        _authorise(con, vessel_id, who)
        alerts = _rows(con, "select * from mart_alerts where vessel_id = ? order by first_flagged desc", [vessel_id])
        for a in alerts:
            a["fault_match"] = _rows(
                con,
                "select fault, similarity, rank, confident, margin from mart_fault_match where alert_id = ? order by rank",
                [a["alert_id"]],
            )
        return alerts


@app.get("/vessels/{vessel_id}/sensors/{sensor_id}")
def sensor_series(
    vessel_id: str,
    sensor_id: str,
    who: Owner,
    start: Annotated[datetime, Query(alias="from")],
    end: Annotated[datetime, Query(alias="to")],
    resolution: Literal["auto", "1min", "1h"] = "auto",
) -> dict:
    if end <= start:
        raise HTTPException(status_code=422, detail="'to' must be after 'from'")
    if end - start > timedelta(days=90):
        raise HTTPException(status_code=422, detail="range limited to 90 days")
    res = resolution if resolution != "auto" else ("1min" if end - start <= timedelta(days=2) else "1h")
    bucket = "minute" if res == "1min" else "hour"
    with _db() as con:
        _authorise(con, vessel_id, who)
        if con.execute("select 1 from sensor_registry where sensor_id = ?", [sensor_id]).fetchone() is None:
            raise HTTPException(status_code=404, detail="unknown sensor")
        points = _rows(
            con,
            f"""
            select date_trunc('{bucket}', minute) as ts, min(min_value) as min,
                   sum(mean_value * n) / sum(n) as mean, max(max_value) as max, sum(n) as n
            from mart_rollup_1min
            where vessel_id = ? and sensor_id = ? and minute >= ? and minute < ?
            group by 1 order by 1""",
            [vessel_id, sensor_id, start, end],
        )
    return {"vessel_id": vessel_id, "sensor_id": sensor_id, "resolution": res, "points": points}


@app.get("/vessels/{vessel_id}/dq")
def vessel_dq(vessel_id: str, who: Owner) -> list[dict]:
    with _db() as con:
        _authorise(con, vessel_id, who)
        return _rows(con, "select * from mart_dq_daily where vessel_id = ? order by day", [vessel_id])
