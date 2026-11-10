-- One row per vessel: owner, latest health, open alerts, $ at risk, freshness.
with as_of as (select max(sent_at) as as_of_utc from {{ ref('stg_manifest') }}),
last_data as (select vessel_id, max(minute) as last_reading_utc from {{ ref('mart_rollup_1min') }} group by all),
latest_health as (
    select vessel_id, system, health_index from {{ ref('mart_system_health') }}
    qualify row_number() over (partition by vessel_id, system order by hour desc) = 1
),
alerts as (
    select vessel_id,
        count(*) filter (where status = 'open') as open_alerts,
        count(*) as total_alerts,
        coalesce(sum(usd_at_risk) filter (where status = 'open'), 0) as usd_at_risk_open
    from {{ ref('mart_alerts') }} group by all
)
select
    f.vessel_id, f.owner, f.logger,
    (select round(min(health_index), 1) from latest_health h where h.vessel_id = f.vessel_id) as health_index,
    coalesce(a.open_alerts, 0) as open_alerts,
    coalesce(a.total_alerts, 0) as total_alerts,
    coalesce(a.usd_at_risk_open, 0) as usd_at_risk_open,
    l.last_reading_utc,
    (select as_of_utc from as_of) as as_of_utc,
    round(epoch((select as_of_utc from as_of) - l.last_reading_utc) / 3600.0, 1) as freshness_lag_hours
from {{ ref('fleet') }} f
left join last_data l using (vessel_id)
left join alerts a using (vessel_id)
