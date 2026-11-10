-- Health index per vessel x system x hour: 100 - clamp(mean |z| x k).
select
    vessel_id, system, hour,
    greatest(0, 100 - least(100, avg(abs(z)) * {{ var('health_k') }})) as health_index,
    max(abs(z)) as max_abs_z,
    count(*) filter (where abs(z) >= {{ var('z_threshold') }}) as n_sensors_flagged,
    count(*) as n_sensors_scored,
    string_agg(sensor_id, ',' order by abs(z) desc) filter (where abs(z) >= {{ var('z_threshold') }}) as flagged_sensors
from {{ ref('mart_anomaly_scores') }}
group by all
