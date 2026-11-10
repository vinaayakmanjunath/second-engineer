-- What "normal" looks like, per vessel x sensor: a straight line against engine speed
-- (value = intercept + slope x rpm) fitted on each system's first baseline_days of running,
-- with a robust sigma = 1.4826 x MAD of the residuals (floored at sigma_floor_frac x median).
-- Constant-speed machines (generators: rpm spread < 5%) get a flat baseline (the median) and no
-- speed-range limit; variable-speed engines are only scored inside the rpm range they were learned on.
with first_run as (
    select vessel_id, system, min(hour) as first_hour
    from {{ ref('int_sensor_hourly') }} group by all
),
ref_window as (
    select s.*
    from {{ ref('int_sensor_hourly') }} s join first_run f using (vessel_id, system)
    where s.hour < f.first_hour + interval ({{ var('baseline_days') }} * 24) hour
),
stats as (
    select vessel_id, sensor_id, system, measurement,
        regr_intercept(value, rpm) as fit_intercept,
        regr_slope(value, rpm) as fit_slope,
        quantile_cont(rpm, 0.01) as rpm_p01,
        quantile_cont(rpm, 0.99) as rpm_p99,
        median(rpm) as rpm_median,
        median(value) as median_value,
        count(*) as n_hours,
        min(hour) as ref_start,
        max(hour) as ref_end
    from ref_window group by all
    having count(*) >= {{ var('baseline_min_hours') }}
),
fit as (
    select *,
        (rpm_p99 - rpm_p01) >= 0.05 * rpm_median as speed_conditioned,
        case when (rpm_p99 - rpm_p01) >= 0.05 * rpm_median then fit_intercept else median_value end as intercept,
        case when (rpm_p99 - rpm_p01) >= 0.05 * rpm_median then fit_slope else 0 end as slope,
        case when (rpm_p99 - rpm_p01) >= 0.05 * rpm_median then rpm_p01 end as rpm_min,
        case when (rpm_p99 - rpm_p01) >= 0.05 * rpm_median then rpm_p99 end as rpm_max
    from stats
),
resid as (
    select f.vessel_id, f.sensor_id,
        1.4826 * median(abs(r.value - (f.intercept + f.slope * r.rpm))) as robust_sigma
    from fit f join ref_window r using (vessel_id, sensor_id)
    group by all
)
select f.vessel_id, f.sensor_id, f.system, f.measurement, f.speed_conditioned,
    f.intercept, f.slope, f.rpm_min, f.rpm_max, f.median_value, f.n_hours, f.ref_start, f.ref_end,
    greatest(r.robust_sigma, {{ var('sigma_floor_frac') }} * abs(f.median_value)) as sigma,
    r.robust_sigma,
    -- which term set sigma: the floor binds when residuals are tiny or quantised
    case when r.robust_sigma < {{ var('sigma_floor_frac') }} * abs(f.median_value) then 'floor' else 'mad' end
        as sigma_source
from fit f join resid r using (vessel_id, sensor_id)
where greatest(r.robust_sigma, {{ var('sigma_floor_frac') }} * abs(f.median_value)) > 0
