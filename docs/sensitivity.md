# Sensitivity: detector response vs fault severity (generated)

_Generated 2026-11-10 15:48 UTC by `secondeng sensitivity`._

Vessel GLORIA-C, system ME, fault `air_cooler_fouling`, 96 h ramp. Severity = deviation at failure as a multiple of the deviation measured on the test bench. Severity 0 is the control (no fault): any alert there is a false alarm.

| severity x bench | alerts opened | first alert, hours before failure | flagged hours (>= min sensors at abs z >= threshold) | peak max abs z | strongest alert: top fault match | strongest alert: rank of the injected fault | alerts before the ramp (false alarms, identical by construction) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 3 | 65.7 | 17 | 6.84 | turbine_degradation | 2 | 5 |
| 1.0 | 3 | 66.7 | 35 | 9.16 | turbine_degradation | 2 | 5 |
| 2.0 | 3 | 66.7 | 37 | 12.16 | turbine_degradation | 2 | 5 |
| 3.0 | 3 | 66.7 | 38 | 15.15 | air_cooler_fouling | 1 | 5 |
| 5.0 | 3 | 66.7 | 41 | 21.15 | air_cooler_fouling | 1 | 5 |

Compare each row with severity 0: only differences from the control are effects of the fault.
