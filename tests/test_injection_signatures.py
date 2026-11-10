from datetime import datetime, timedelta

import polars as pl
import pytest

from secondeng import inject, signatures


def _sig():
    return pl.DataFrame(
        {
            "fault": ["f"] * 2,
            "load_pct": [60, 85],
            "measurement": ["exh_temp"] * 2,
            "rel_dev": [0.02, 0.04],
            "pre_mean": [1.0, 1.0],
            "anomaly_mean": [1.0, 1.0],
            "source_file": ["a", "b"],
        }
    )


def test_ramp_zero_before_full_at_failure_and_only_while_running(s):
    s.injection.noise_frac = 0.0
    start = datetime(2026, 9, 1)
    ts = [start + timedelta(hours=h) for h in range(0, 200)]
    me = pl.DataFrame(
        {"ts_local": ts, "engine_rpm": [700.0] * 199 + [0.0], "cyl_1_temp_c": [400.0] * 200, "oil_temp_c": [70.0] * 200}
    )
    failure = start + timedelta(hours=150)
    gt = {
        "fault": "f",
        "ramp_start_local": (failure - timedelta(hours=96)).isoformat(),
        "failure_local": failure.isoformat(),
    }
    out = inject.apply(s, me, _sig(), gt)
    sev = s.injection.severity_x_bench
    by_t = dict(zip(out["ts_local"], out["cyl_1_temp_c"], strict=True))
    assert by_t[start + timedelta(hours=10)] == pytest.approx(400.0)  # before ramp
    assert by_t[failure - timedelta(hours=48)] == pytest.approx(400 * (1 + 0.03 * sev * 0.5))
    assert by_t[failure - timedelta(hours=1)] == pytest.approx(400 * (1 + 0.03 * sev * 95 / 96))
    assert out["ts_local"].max() < failure  # engine down after failure
    assert out["oil_temp_c"].unique().to_list() == [70.0]  # not in signature


def test_signature_sign_and_magnitude_from_three_row_header_csv(s, tmp_path):
    path = tmp_path / "bench.csv"
    header = [
        "Time_abs,Time_rel,Anomaly State,No.1 Exh.Gas Temp.,No.2 Exh.Gas Temp.,No.3 Exh.Gas Temp.,"
        "Cooling Water Temp. Engine Out I,LO Temp. Engine Out,Engine Speed,Charge Air Press.",
        ",,,T1,T2,T3,T7,T11,N,Pturb",
        "sec,sec,,°C,°C,°C,°C,°C,rpm,kgf/cm2",
    ]
    rows = [
        "0,0,0,400,400,400,60,50,1000,1",
        "1,1,0,400,400,400,60,50,1000,1",
        "2,2,1,440,440,440,60,45,1000,1",
        "3,3,1,440,440,440,60,45,1000,1",
    ]
    path.write_text("\n".join(header + rows) + "\n")
    m = signatures.to_measurements(signatures.read_bench_csv(path), s)
    pre, ano = m.filter(pl.col("anomaly") == 0), m.filter(pl.col("anomaly") == 1)
    assert (ano["exh_temp"].mean() - pre["exh_temp"].mean()) / pre["exh_temp"].mean() == pytest.approx(0.10)
    assert (ano["lo_temp"].mean() - pre["lo_temp"].mean()) / pre["lo_temp"].mean() == pytest.approx(-0.10)
    assert "speed" not in m.columns  # load proxy is not part of a fault signature
