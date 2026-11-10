"""How big must the fault be? Re-run the injected vessel at several severities -> docs/sensitivity.md.

Severity 0 is the control: same vessel, same detector, no fault. Each run is isolated in
data/sensitivity/x<k>/ and uses only the injected vessel (detection is per vessel).
"""

from __future__ import annotations

from pathlib import Path

from secondeng import evaluate, lake
from secondeng.settings import ROOT, Settings

SEVERITIES = [0.0, 1.0, 2.0, 3.0, 5.0]


def write(main: Settings) -> Path:
    runs = [evaluate.isolated_run(main, k, ROOT / "data" / "sensitivity" / f"x{k:g}") for k in SEVERITIES]
    gt = evaluate.ground_truth(runs[0])
    control_wh = runs[0].path(runs[0].warehouse)  # severity 0
    rows = []
    for s in runs:
        ph = evaluate.paired_hours(s.path(s.warehouse), control_wh, gt, main.detection.min_sensors)
        rows.append(
            evaluate.window_metrics(s)
            | {"fault_only_hours": ph["fault_only"], "control_only_hours": ph["control_only"]}
        )
    labels = [
        *evaluate.METRIC_LABELS,
        ("fault_only_hours", "hours flagged only with the fault"),
        ("control_only_hours", "hours flagged only in the control"),
    ]
    keys = ["severity_x_bench", *(k for k, _ in labels)]
    out = [
        "# Sensitivity: detector response vs fault severity (generated)",
        "",
        f"_Generated {lake.now_utc():%Y-%m-%d %H:%M} UTC by `secondeng sensitivity`._",
        "",
        f"Vessel {main.injection.vessel_id}, system ME, fault `{main.injection.fault}`, "
        f"{main.injection.ramp_hours:g} h ramp. Severity = deviation at failure as a multiple of the deviation "
        "measured on the test bench. Severity 0 is the control (no fault): any alert there is a false alarm.",
        "",
        "| severity x bench | " + " | ".join(name for _, name in labels) + " |",
        "|" + "---:|" * len(keys),
    ]
    out += ["| " + " | ".join("-" if r[k] is None else str(r[k]) for k in keys) + " |" for r in rows]
    out += [
        "",
        "Compare each row with severity 0: only differences from the control are effects of the fault. The last two "
        "columns pair the same hours of the same vessel with and without the fault.",
        "",
    ]
    path = main.path(Path("docs/sensitivity.md"))
    lake.write_text_atomic("\n".join(out), path)
    return path
