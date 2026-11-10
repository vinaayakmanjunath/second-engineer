# Results (generated)

_Generated 2026-11-10T16:00:03Z by `secondeng evaluate`. Do not edit by hand._

## Injected fault (ground truth, never read by the pipeline)

- Vessel / system: **GLORIA-C / ME**
- Fault: **air_cooler_fouling** (bench signature x 3.0)
- Ramp: 96.0 h from 2026-09-22 13:40:59 UTC to declared failure 2026-09-26 13:40:59 UTC

## Fault vs no-fault control (same vessel, same detector)

**No lead time claimed: the no-fault control also opens an alert in the ramp window.**

| measure (ramp window) | with fault | control (no fault) |
|---|---:|---:|
| alerts opened | 3 | 3 |
| first alert, hours before failure | 66.7 | 65.7 |
| flagged hours (>= min sensors at abs z >= threshold) | 38 | 17 |
| peak max abs z | 15.16 | 6.84 |
| strongest alert: top fault match | air_cooler_fouling | turbine_degradation |
| strongest alert: rank of the injected fault | 1 | 2 |
| alerts before the ramp (false alarms, identical by construction) | 5 | 5 |

## Alerts not explained by the injection

| vessel | alerts | days with running data | per running day |
|---|---:|---:|---:|
| GLORIA-A | 6 | 24 | 0.25 |
| GLORIA-B | 1 | 13 | 0.077 |
| GLORIA-C | 5 | 24 | 0.208 |

## Transport mess: injected vs handled

| vessel | check | injected | handled | exact match |
|---|---|---:|---:|:---:|
| GLORIA-A | identical re-sends dropped (sha256, bronze) | 19 | 19 | yes |
| GLORIA-A | re-sends with new envelope dropped (batch_id, silver) | 19 | 19 | yes |
| GLORIA-A | blank timestamps quarantined (readings) | 8,676 | 8,676 | yes |
| GLORIA-A | future timestamps quarantined (readings) | 9,110 | 9,110 | yes |
| GLORIA-A | readings accounted for (valid + quarantined = sent) | 34,354,734 | 34,354,734 | yes |
| GLORIA-A | late or outage-delayed batches ingested (>1 h after data) | 139 | 133 | n/a |
| GLORIA-A | rows shuffled in transit / order breaks seen on arrival | 49,288 | 47,190 | n/a |
| GLORIA-B | identical re-sends dropped (sha256, bronze) | 19 | 19 | yes |
| GLORIA-B | re-sends with new envelope dropped (batch_id, silver) | 14 | 14 | yes |
| GLORIA-B | blank timestamps quarantined (readings) | 4,576 | 4,576 | yes |
| GLORIA-B | future timestamps quarantined (readings) | 4,715 | 4,715 | yes |
| GLORIA-B | readings accounted for (valid + quarantined = sent) | 19,326,897 | 19,326,897 | yes |
| GLORIA-B | late or outage-delayed batches ingested (>1 h after data) | 51 | 51 | n/a |
| GLORIA-B | rows shuffled in transit / order breaks seen on arrival | 27,483 | 26,290 | n/a |
| GLORIA-C | identical re-sends dropped (sha256, bronze) | 20 | 20 | yes |
| GLORIA-C | re-sends with new envelope dropped (batch_id, silver) | 24 | 24 | yes |
| GLORIA-C | blank timestamps quarantined (readings) | 6,883 | 6,883 | yes |
| GLORIA-C | future timestamps quarantined (readings) | 7,246 | 7,246 | yes |
| GLORIA-C | readings accounted for (valid + quarantined = sent) | 28,516,155 | 28,516,155 | yes |
| GLORIA-C | late or outage-delayed batches ingested (>1 h after data) | 142 | 134 | n/a |
| GLORIA-C | rows shuffled in transit / order breaks seen on arrival | 45,518 | 43,206 | n/a |

Late/out-of-order rows are informational: delays overlap with outages, and a shuffled row may land in its original position.
