"""Owner-scoped read API over the `serving` schema in Postgres (published from gold by `secondeng publish`).

Auth: header X-API-Key. Keys come from env API_KEYS='{"<key>": "<owner>", ...}' (never from the repo).
Unknown/missing key -> 401. A vessel owned by someone else -> 403. Unknown vessel -> 404.
The database connection (DATABASE_URL) uses a read-only role; bad configuration fails at startup,
not on the first request.
"""

from __future__ import annotations

import hmac
import json
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.security import APIKeyHeader
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


def _keys() -> dict[str, str]:
    try:
        keys = json.loads(os.environ.get("API_KEYS", ""))
    except json.JSONDecodeError as e:
        raise RuntimeError("API_KEYS must be a JSON object of key -> owner") from e
    if not isinstance(keys, dict) or not keys:
        raise RuntimeError("API_KEYS must be a non-empty JSON object of key -> owner")
    return {str(k): str(v) for k, v in keys.items()}


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.keys = _keys()
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    # check= replaces connections the serverless database dropped while idle
    with ConnectionPool(
        url,
        min_size=1,
        max_size=4,
        kwargs={"row_factory": dict_row, "autocommit": True},
        check=ConnectionPool.check_connection,
    ) as pool:
        pool.wait(timeout=10.0)  # a wrong or unreachable database fails the deploy here, not on a request
        app.state.pool = pool
        yield


app = FastAPI(
    title="Second Engineer API",
    version="0.2.0",
    description="Demonstration on open data: virtual vessels, injected fault, not validated.",
    lifespan=lifespan,
)

api_key = APIKeyHeader(name="X-API-Key", auto_error=False)


def owner(request: Request, key: Annotated[str | None, Depends(api_key)]) -> str:
    if key:
        for k, who in request.app.state.keys.items():
            if hmac.compare_digest(k.encode(), key.encode()):
                return who
    raise HTTPException(status_code=401, detail="missing or invalid API key")


Owner = Annotated[str, Depends(owner)]


def _rows(request: Request, sql: str, params: list) -> list[dict]:
    with request.app.state.pool.connection() as con:
        return con.execute(sql, params).fetchall()


def _authorise(request: Request, vessel_id: str, who: str) -> None:
    rows = _rows(request, "select owner from serving.fleet where vessel_id = %s", [vessel_id])
    if not rows:
        raise HTTPException(status_code=404, detail="unknown vessel")
    if rows[0]["owner"] != who:
        raise HTTPException(status_code=403, detail="vessel belongs to another owner")


def _utc(t: datetime) -> datetime:
    """Timestamps are stored as naive UTC: treat a naive input as UTC, convert a zoned one."""
    return t if t.tzinfo is None else t.astimezone(UTC).replace(tzinfo=None)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/vessels")
def vessels(request: Request, who: Owner) -> list[dict]:
    return _rows(request, "select * from serving.mart_fleet where owner = %s order by vessel_id", [who])


@app.get("/vessels/{vessel_id}/health")
def vessel_health(
    request: Request, vessel_id: str, who: Owner, hours: int = Query(168, ge=1, le=24 * 90)
) -> list[dict]:
    _authorise(request, vessel_id, who)
    return _rows(
        request,
        """
        select system, hour, round(health_index::numeric, 1)::float8 as health_index, max_abs_z, n_sensors_flagged
        from serving.mart_system_health
        where vessel_id = %s
          and hour >= (select max(hour) from serving.mart_system_health where vessel_id = %s) - %s * interval '1 hour'
        order by system, hour""",
        [vessel_id, vessel_id, hours],
    )


@app.get("/vessels/{vessel_id}/alerts")
def vessel_alerts(request: Request, vessel_id: str, who: Owner) -> list[dict]:
    _authorise(request, vessel_id, who)
    return _rows(
        request,
        """
        select a.*, coalesce(
            (select json_agg(json_build_object('fault', m.fault, 'similarity', m.similarity, 'rank', m.rank,
                                               'confident', m.confident, 'margin', m.margin) order by m.rank)
             from serving.mart_fault_match m where m.alert_id = a.alert_id), '[]'::json) as fault_match
        from serving.mart_alerts a where a.vessel_id = %s order by a.first_flagged desc""",
        [vessel_id],
    )


@app.get("/vessels/{vessel_id}/sensors/{sensor_id}")
def sensor_series(
    request: Request,
    vessel_id: str,
    sensor_id: str,
    who: Owner,
    start: Annotated[datetime, Query(alias="from")],
    end: Annotated[datetime, Query(alias="to")],
    resolution: Literal["auto", "1min", "1h"] = "auto",
) -> dict:
    start, end = _utc(start), _utc(end)
    if end <= start:
        raise HTTPException(status_code=422, detail="'to' must be after 'from'")
    if end - start > timedelta(days=90):
        raise HTTPException(status_code=422, detail="range limited to 90 days")
    res = resolution if resolution != "auto" else ("1min" if end - start <= timedelta(days=2) else "1h")
    _authorise(request, vessel_id, who)
    if not _rows(request, "select 1 from serving.sensor_registry where sensor_id = %s", [sensor_id]):
        raise HTTPException(status_code=404, detail="unknown sensor")
    points = _rows(
        request,
        """
        select date_trunc(%s, minute) as ts, min(min_value) as min,
               sum(mean_value * n) / sum(n) as mean, max(max_value) as max, sum(n)::bigint as n
        from serving.mart_rollup_1min
        where vessel_id = %s and sensor_id = %s and minute >= %s and minute < %s
        group by 1 order by 1""",
        ["minute" if res == "1min" else "hour", vessel_id, sensor_id, start, end],
    )
    return {"vessel_id": vessel_id, "sensor_id": sensor_id, "resolution": res, "points": points}


@app.get("/vessels/{vessel_id}/dq")
def vessel_dq(request: Request, vessel_id: str, who: Owner) -> list[dict]:
    _authorise(request, vessel_id, who)
    return _rows(request, "select * from serving.mart_dq_daily where vessel_id = %s order by day", [vessel_id])
