-- Per vessel x system x minute: operating state from the speed channel.
-- running = speed above running_frac x p99 rpm for the last running_min_minutes minutes.
with speed as (
    select vessel_id, system, minute, mean_value as rpm
    from {{ ref('mart_rollup_1min') }}
    where measurement = 'speed'
),
rated as (
    select vessel_id, system, quantile_cont(rpm, 0.99) as p99_rpm from speed group by all
),
flagged as (
    select s.*, r.p99_rpm,
        sum(case when s.rpm > {{ var('running_frac_of_p99_rpm') }} * r.p99_rpm then 1 else 0 end) over (
            partition by s.vessel_id, s.system order by s.minute
            range between interval ({{ var('running_min_minutes') - 1 }}) minute preceding and current row
        ) as minutes_above
    from speed s join rated r using (vessel_id, system)
)
select
    vessel_id, system, minute, rpm, p99_rpm,
    100.0 * rpm / nullif(p99_rpm, 0) as load_pct,
    case
        when minutes_above >= {{ var('running_min_minutes') }} then 'running'
        when rpm > 0.1 * p99_rpm then 'idle'
        else 'off'
    end as state
from flagged
