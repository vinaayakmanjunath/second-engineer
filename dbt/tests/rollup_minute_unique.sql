select vessel_id, sensor_id, minute, count(*) from {{ ref('mart_rollup_1min') }}
group by all having count(*) > 1
