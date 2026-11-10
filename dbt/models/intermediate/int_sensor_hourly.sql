-- Hourly means per scored sensor, kept only for hours the system was running throughout
-- (>= 30 minutes of data, every minute 'running'), with the hour's mean engine speed.
with joined as (
    select r.vessel_id, r.sensor_id, r.system, r.measurement,
        date_trunc('hour', r.minute) as hour, r.mean_value, r.n, o.state, o.rpm
    from {{ ref('mart_rollup_1min') }} r
    join {{ ref('int_operating_state') }} o using (vessel_id, system, minute)
    where r.measurement not in (
        {% for m in var('unscored_measurements') %}'{{ m }}'{% if not loop.last %}, {% endif %}{% endfor %}
    )
)
select
    vessel_id, sensor_id, system, measurement, hour,
    sum(mean_value * n) / sum(n) as value,
    avg(rpm) as rpm,
    count(*) as minutes
from joined
group by all
having bool_and(state = 'running') and count(*) >= 30
