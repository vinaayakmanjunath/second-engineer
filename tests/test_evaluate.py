from datetime import datetime, timedelta

import duckdb

from secondeng import evaluate

T0 = datetime(2026, 9, 22, 0)


def _health(path, flags_by_hour):
    con = duckdb.connect(str(path))
    con.execute(
        "create table mart_system_health (vessel_id varchar, system varchar, hour timestamp, n_sensors_flagged int)"
    )
    con.executemany(
        "insert into mart_system_health values ('GLORIA-C', 'ME', ?, ?)",
        [[T0 + timedelta(hours=h), n] for h, n in flags_by_hour.items()],
    )
    con.close()
    return path


def test_paired_hours_counts_each_hour_once_against_the_control(tmp_path):
    # hours -2,-1 are before the ramp; 0..3 are in the window; min_sensors = 3
    fault = _health(tmp_path / "f.duckdb", {-2: 0, -1: 3, 0: 3, 1: 4, 2: 0, 3: 1})
    control = _health(tmp_path / "c.duckdb", {-2: 0, -1: 3, 0: 3, 1: 0, 2: 5, 3: 0})
    gt = {"vessel_id": "GLORIA-C", "system": "ME", "ramp_start_utc": T0, "failure_utc": T0 + timedelta(hours=4)}
    p = evaluate.paired_hours(fault, control, gt, min_sensors=3)
    assert p == {"both": 1, "fault_only": 1, "control_only": 1, "neither": 1, "control_pre_ramp_flag_rate": 0.5}
