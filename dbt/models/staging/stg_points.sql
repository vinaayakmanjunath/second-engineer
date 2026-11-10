-- Typed readings joined to the canonical registry.
select
    p.vessel_id,
    cast(p.date as date) as date,
    p.ts_utc,
    p.sensor_id,
    r.system,
    r.measurement,
    p.value,
    p.batch_id,
    p.sent_at,
    p.ingested_at
from {{ source('silver', 'points') }} p
join {{ ref('sensor_registry') }} r on r.sensor_id = p.sensor_id
