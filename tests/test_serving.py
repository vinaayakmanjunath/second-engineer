"""Serving path end to end on real Postgres: publish gold -> serving schema -> API as a read-only role.

Needs TEST_DATABASE_URL (a superuser on a scratch database). Skipped locally without it; in CI
(CI=true) a missing database is a failure, so tenancy tests can never silently disappear.
"""

from __future__ import annotations

import os

import duckdb
import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.conninfo import make_conninfo

from secondeng import publish
from secondeng.settings import ROOT

FIXTURE_SQL = """
create table fleet as select * from (values ('GLORIA-A', 'owner_north', 'gloria_v1'),
    ('GLORIA-C', 'owner_south', 'gloria_v1')) t(vessel_id, owner, logger);
create table sensor_registry as select 'me.exh_temp_cyl3_c' as sensor_id, 'ME' as system,
    'exh_temp' as measurement, 'degC' as unit, 0 as min_value, 600 as max_value;
create table mart_fleet as select vessel_id, owner, logger, 90.5 as health_index, 1::bigint as open_alerts,
    2::bigint as total_alerts, 1000::bigint as usd_at_risk_open, timestamp '2026-09-25 00:00:00' as last_reading_utc,
    timestamp '2026-09-25 01:00:00' as as_of_utc, 1.0 as freshness_lag_hours from fleet;
create table mart_system_health as select 'GLORIA-C' as vessel_id, 'ME' as system,
    timestamp '2026-09-24 00:00:00' + to_hours(i) as hour, 88.04 as health_index, 3.5 as max_abs_z,
    3::bigint as n_sensors_flagged, 9::bigint as n_sensors_scored, 'x' as flagged_sensors from range(3) t(i);
create table mart_alerts as select 'A1' as alert_id, 'GLORIA-C' as vessel_id, 'ME' as system,
    timestamp '2026-09-24 00:00:00' as first_flagged, timestamp '2026-09-24 02:00:00' as opened_at,
    timestamp '2026-09-24 03:00:00' as last_seen, 3::bigint as flagged_hours, 15.15 as max_abs_z, 40.0 as min_health,
    'high' as severity, 'open' as status, 'a,b,c' as sensors, 1000::bigint as usd_at_risk;
create table mart_fault_match as select * from (values ('A1', 'air_cooler_fouling', 0.998, 1::bigint, true, 0.002),
    ('A1', 'turbine_degradation', 0.996, 2::bigint, true, null)) t(alert_id, fault, similarity, rank, confident, margin);
create table mart_dq_daily as select vessel_id, date '2026-09-24' as day, 100::bigint as points_valid,
    1::bigint as points_quarantined, 99.0 as valid_pct, 98.0 as coverage_pct, 1::bigint as batches_loaded,
    0::bigint as duplicate_batches, 0::bigint as duplicate_files, 0::bigint as late_batches,
    2.0 as lateness_p95_min, 0::bigint as out_of_order_rows, 0::bigint as flatlined_sensor_hours from fleet;
create table mart_rollup_1min as select 'GLORIA-C' as vessel_id, 'me.exh_temp_cyl3_c' as sensor_id,
    timestamp '2026-09-24 00:00:00' + to_minutes(i) as minute, 300.0 as min_value, 310.0 as mean_value,
    320.0 as max_value, 60::bigint as n from range(3) t(i);
"""


@pytest.fixture
def pg_admin():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        if os.environ.get("CI"):
            pytest.fail("TEST_DATABASE_URL must be set in CI")
        pytest.skip("TEST_DATABASE_URL not set")
    with psycopg.connect(url, autocommit=True) as con:
        con.execute("drop schema if exists serving cascade")
        con.execute("drop schema if exists serving_next cascade")
        con.execute((ROOT / "deploy" / "neon_roles.sql").read_text())
        con.execute("alter role loader password 'loader-test'")
        con.execute("alter role api_reader password 'reader-test'")
    return url


@pytest.fixture
def urls(pg_admin):
    return {
        "loader": make_conninfo(pg_admin, user="loader", password="loader-test"),
        "reader": make_conninfo(pg_admin, user="api_reader", password="reader-test"),
        "admin": pg_admin,
    }


@pytest.fixture
def warehouse(tmp_path):
    wh = tmp_path / "wh.duckdb"
    with duckdb.connect(str(wh)) as con:
        con.execute(FIXTURE_SQL)
    return wh


def _count(url, table):
    with psycopg.connect(url) as con:
        return con.execute(f"select count(*) from serving.{table}").fetchone()[0]


def test_publish_copies_exactly_the_served_columns(urls, warehouse):
    counts = publish.publish(warehouse, urls["loader"])
    assert counts == {
        "fleet": 2,
        "sensor_registry": 1,
        "mart_fleet": 2,
        "mart_system_health": 3,
        "mart_alerts": 1,
        "mart_fault_match": 2,
        "mart_dq_daily": 2,
        "mart_rollup_1min": 3,
    }
    with psycopg.connect(urls["admin"]) as con:
        for table, cols in publish.SERVED.items():
            got = [
                r[0]
                for r in con.execute(
                    "select column_name from information_schema.columns where table_schema = 'serving' "
                    "and table_name = %s order by ordinal_position",
                    [table],
                ).fetchall()
            ]
            assert got == cols, table


def test_failed_publish_leaves_live_serving_untouched(urls, warehouse, monkeypatch):
    publish.publish(warehouse, urls["loader"])
    monkeypatch.setitem(publish.SERVED, "mart_missing", ["x"])  # fails after the first tables are copied
    with pytest.raises(duckdb.CatalogException):
        publish.publish(warehouse, urls["loader"])
    assert _count(urls["admin"], "mart_rollup_1min") == 3
    assert _count(urls["admin"], "fleet") == 2


def test_api_role_can_read_but_not_write(urls, warehouse):
    publish.publish(warehouse, urls["loader"])
    assert _count(urls["reader"], "fleet") == 2
    with psycopg.connect(urls["reader"]) as con, pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        con.execute("insert into serving.fleet values ('X', 'Y')")


@pytest.fixture
def client(urls, warehouse, monkeypatch):
    publish.publish(warehouse, urls["loader"])
    monkeypatch.setenv("DATABASE_URL", urls["reader"])
    monkeypatch.setenv("API_KEYS", '{"k-north": "owner_north", "k-south": "owner_south"}')
    from secondeng.api import app

    with TestClient(app) as c:
        yield c


NORTH, SOUTH = {"X-API-Key": "k-north"}, {"X-API-Key": "k-south"}


def test_no_or_bad_key_is_401(client):
    assert client.get("/vessels").status_code == 401
    assert client.get("/vessels", headers={"X-API-Key": "nope"}).status_code == 401


def test_owner_sees_only_own_fleet(client):
    r = client.get("/vessels", headers=NORTH)
    assert r.status_code == 200
    assert [v["vessel_id"] for v in r.json()] == ["GLORIA-A"]


@pytest.mark.parametrize(
    "path", ["dq", "alerts", "health", "sensors/me.exh_temp_cyl3_c?from=2026-09-24T00:00:00&to=2026-09-25T00:00:00"]
)
def test_every_vessel_endpoint_enforces_ownership(client, path):
    assert client.get(f"/vessels/GLORIA-C/{path}", headers=NORTH).status_code == 403
    assert client.get(f"/vessels/GLORIA-Z/{path}", headers=SOUTH).status_code == 404
    assert client.get(f"/vessels/GLORIA-C/{path}", headers=SOUTH).status_code == 200


def test_alerts_carry_their_fault_matches(client):
    [a] = client.get("/vessels/GLORIA-C/alerts", headers=SOUTH).json()
    assert [m["fault"] for m in a["fault_match"]] == ["air_cooler_fouling", "turbine_degradation"]
    assert a["fault_match"][0]["confident"] is True and a["usd_at_risk"] == 1000


def test_health_window_and_rounding(client):
    rows = client.get("/vessels/GLORIA-C/health?hours=1", headers=SOUTH).json()
    assert [r["hour"] for r in rows] == ["2026-09-24T01:00:00", "2026-09-24T02:00:00"]
    assert rows[0]["health_index"] == 88.0


@pytest.mark.parametrize("frm", ["2026-09-24T00:00:00", "2026-09-24T00:00:00Z", "2026-09-24T01:00:00+01:00"])
def test_sensor_series_accepts_naive_or_zoned_times_as_utc(client, frm):
    r = client.get(
        "/vessels/GLORIA-C/sensors/me.exh_temp_cyl3_c",
        params={"from": frm, "to": "2026-09-24T00:02:00Z"},
        headers=SOUTH,
    )
    assert r.status_code == 200
    body = r.json()
    assert body["resolution"] == "1min" and [p["n"] for p in body["points"]] == [60, 60]


def test_unknown_sensor_is_404(client):
    r = client.get("/vessels/GLORIA-C/sensors/nope?from=2026-09-24T00:00:00&to=2026-09-25T00:00:00", headers=SOUTH)
    assert r.status_code == 404


def test_health_needs_no_key(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_bad_api_keys_config_fails_at_startup(urls, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", urls["reader"])
    monkeypatch.setenv("API_KEYS", "not json")
    from secondeng.api import app

    with pytest.raises(RuntimeError, match="API_KEYS"), TestClient(app):
        pass
