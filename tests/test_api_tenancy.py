import duckdb
import pytest
from fastapi.testclient import TestClient

from secondeng.api import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    wh = tmp_path / "wh.duckdb"
    con = duckdb.connect(str(wh))
    con.execute(
        "create table fleet as select * from (values ('GLORIA-A','owner_north'), ('GLORIA-C','owner_south')) t(vessel_id, owner)"
    )
    con.execute("create table mart_fleet as select vessel_id, owner, 90.0 as health_index from fleet")
    con.execute(
        "create table mart_dq_daily as select 'GLORIA-A' as vessel_id, date '2026-09-01' as day, 99.0 as valid_pct"
    )
    con.close()
    monkeypatch.setenv("SECONDENG_WAREHOUSE", str(wh))
    monkeypatch.setenv("API_KEYS", '{"k-north": "owner_north", "k-south": "owner_south"}')
    return TestClient(app)


def test_no_or_bad_key_is_401(client):
    assert client.get("/vessels").status_code == 401
    assert client.get("/vessels", headers={"X-API-Key": "nope"}).status_code == 401


def test_owner_sees_only_own_fleet(client):
    r = client.get("/vessels", headers={"X-API-Key": "k-north"})
    assert r.status_code == 200
    assert [v["vessel_id"] for v in r.json()] == ["GLORIA-A"]


def test_other_owners_vessel_is_403_unknown_is_404(client):
    assert client.get("/vessels/GLORIA-A/dq", headers={"X-API-Key": "k-south"}).status_code == 403
    assert client.get("/vessels/GLORIA-Z/dq", headers={"X-API-Key": "k-south"}).status_code == 404
    assert client.get("/vessels/GLORIA-A/dq", headers={"X-API-Key": "k-north"}).status_code == 200


def test_health_needs_no_key(client):
    assert client.get("/health").json() == {"status": "ok"}
