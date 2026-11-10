-- Alerts: >= min_sensors of one system at |z| >= z_threshold in the same hour. Flagged hours
-- no more than alert_close_hours apart form one alert; it opens at the end of the
-- alert_min_hours-th flagged hour. $ at risk = illustrative off-hire event cost x severity weight.
with flagged as (
    select * from {{ ref('mart_system_health') }}
    where n_sensors_flagged >= {{ var('min_sensors') }}
),
gaps as (
    select *,
        case when lag(hour) over w is null
               or hour - lag(hour) over w > interval ({{ var('alert_close_hours') }}) hour
             then 1 else 0 end as is_new
    from flagged
    window w as (partition by vessel_id, system order by hour)
),
islands as (
    select *, sum(is_new) over (partition by vessel_id, system order by hour) as island
    from gaps
),
numbered as (
    select *, row_number() over (partition by vessel_id, system, island order by hour) as k
    from islands
),
alerts as (
    select vessel_id, system, island,
        min(hour) as first_flagged,
        min(hour) filter (where k = {{ var('alert_min_hours') }}) + interval 1 hour as opened_at,
        max(hour) + interval 1 hour as last_seen,
        count(*) as flagged_hours,
        max(max_abs_z) as max_abs_z,
        min(health_index) as min_health
    from numbered group by all
    having count(*) >= {{ var('alert_min_hours') }}
),
latest as (
    select vessel_id, system, max(hour) + interval 1 hour as latest_hour
    from {{ ref('mart_system_health') }} group by all
),
sensors as (
    select a.vessel_id, a.system, a.island, string_agg(distinct z.sensor_id, ',' order by z.sensor_id) as sensors
    from alerts a
    join {{ ref('mart_anomaly_scores') }} z
      on z.vessel_id = a.vessel_id and z.system = a.system
     and z.hour >= a.first_flagged and z.hour < a.last_seen
     and abs(z.z) >= {{ var('z_threshold') }}
    group by all
),
sev as (
    select a.*, s.sensors,
        case when max_abs_z >= 6 then 'high' when max_abs_z >= 4 then 'medium' else 'low' end as severity,
        case when l.latest_hour - a.last_seen <= interval ({{ var('alert_close_hours') }}) hour
             then 'open' else 'closed' end as status
    from alerts a
    join latest l using (vessel_id, system)
    left join sensors s using (vessel_id, system, island)
)
select
    vessel_id || '-' || system || '-' || strftime(first_flagged, '%Y%m%dT%H%M') as alert_id,
    vessel_id, system, first_flagged, opened_at, last_seen, flagged_hours,
    round(max_abs_z, 2) as max_abs_z, round(min_health, 1) as min_health, severity, status, sensors,
    round({{ var('event_usd') }} * case severity
        when 'high' then {{ var('severity_weight')['high'] }}
        when 'medium' then {{ var('severity_weight')['medium'] }}
        else {{ var('severity_weight')['low'] }} end, 0) as usd_at_risk
from sev
