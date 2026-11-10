-- Robust z of every running hour against the speed-conditioned baseline.
-- Hours at engine speeds outside a speed-conditioned baseline's rpm range are not scored
-- (no extrapolation to loads the model never saw).
select
    s.vessel_id, s.sensor_id, s.system, s.measurement, s.hour, s.rpm,
    s.value,
    b.intercept + b.slope * s.rpm as expected,
    b.sigma,
    (s.value - (b.intercept + b.slope * s.rpm)) / b.sigma as z,
    (s.value - (b.intercept + b.slope * s.rpm)) / nullif(abs(b.intercept + b.slope * s.rpm), 0) as rel_dev
from {{ ref('int_sensor_hourly') }} s
join {{ ref('mart_baselines') }} b using (vessel_id, sensor_id)
where not b.speed_conditioned or s.rpm between b.rpm_min and b.rpm_max
