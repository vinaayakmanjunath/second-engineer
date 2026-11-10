-- 1-minute rollups. Incremental: only (vessel, date) partitions with newly ingested silver rows
-- are re-aggregated, then replace their minutes (delete+insert on the unique key).
{{ config(
    materialized='incremental',
    unique_key=['vessel_id', 'sensor_id', 'minute'],
    incremental_strategy='delete+insert'
) }}

with points as (
    select * from {{ ref('stg_points') }}
    {% if is_incremental() %}
    where (vessel_id, date) in (
        select distinct (vessel_id, date) from {{ ref('stg_points') }}
        where ingested_at > (select max(last_ingested_at) from {{ this }})
    )
    {% endif %}
)
select
    vessel_id,
    sensor_id,
    any_value(system) as system,
    any_value(measurement) as measurement,
    date_trunc('minute', ts_utc) as minute,
    min(value) as min_value,
    avg(value) as mean_value,
    max(value) as max_value,
    count(*) as n,
    max(ingested_at) as last_ingested_at
from points
group by vessel_id, sensor_id, date_trunc('minute', ts_utc)
