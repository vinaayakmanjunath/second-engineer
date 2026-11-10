-- Data-quality scorecard per vessel x UTC day.
with pts as (
    select vessel_id, cast(minute as date) as day, (sum(n))::bigint as points_valid
    from {{ ref('mart_rollup_1min') }} group by all
),
cover as (
    select vessel_id, date as day, count(distinct ts_utc) as seconds_with_data
    from {{ ref('stg_points') }} group by all
),
q as (
    select vessel_id, cast(sent_hour as date) as day,
        (sum(n_rows))::bigint as points_quarantined,
        (sum(n_rows) filter (where reason = 'missing_value'))::bigint as q_missing_value,
        (sum(n_rows) filter (where reason = 'bad_timestamp'))::bigint as q_bad_timestamp,
        (sum(n_rows) filter (where reason = 'future_timestamp'))::bigint as q_future_timestamp,
        (sum(n_rows) filter (where reason = 'out_of_range'))::bigint as q_out_of_range,
        (sum(n_rows) filter (where reason = 'unknown_sensor'))::bigint as q_unknown_sensor
    from {{ ref('stg_quarantine') }} group by all
),
b as (
    -- duplicate batches are never parsed (no data timestamps): attribute them to the day they were sent
    select vessel_id, cast(coalesce(min_ts_utc, sent_at) as date) as day,
        count(*) filter (where status = 'loaded') as batches_loaded,
        count(*) filter (where status = 'duplicate_batch') as duplicate_batches,
        count(*) filter (where status = 'loaded' and sent_at - max_ts_utc > interval 1 hour) as late_batches,
        quantile_cont(epoch(sent_at - max_ts_utc) / 60.0, 0.95) filter (where status = 'loaded') as lateness_p95_min,
        (sum(out_of_order_rows))::bigint as out_of_order_rows,
        (sum(n_unmapped))::bigint as unmapped_points
    from {{ ref('stg_batches') }} group by all
),
dupf as (
    select vessel_id, cast(sent_at as date) as day, count(*) as duplicate_files
    from {{ ref('stg_manifest') }} where status = 'duplicate_file' group by all
),
flat as (
    -- sensor-hours with zero variation while the system was running for >= 30 minutes
    select r.vessel_id, cast(date_trunc('hour', r.minute) as date) as day, count(*) as flatlined_sensor_hours
    from (
        select r.vessel_id, r.sensor_id, date_trunc('hour', r.minute) as hr,
            max(r.max_value) - min(r.min_value) as spread, count(*) as minutes, any_value(r.minute) as minute
        from {{ ref('mart_rollup_1min') }} r
        join {{ ref('int_operating_state') }} o using (vessel_id, system, minute)
        where o.state = 'running' and r.measurement <> 'speed'
        group by all
    ) r
    where spread = 0 and minutes >= 30
    group by all
),
days as (
    -- every vessel-day any input mentions, so nothing is dropped by a day with no valid readings
    select vessel_id, day from pts union select vessel_id, day from q
    union select vessel_id, day from b union select vessel_id, day from dupf
)
select
    days.vessel_id, days.day, coalesce(pts.points_valid, 0) as points_valid,
    coalesce(q.points_quarantined, 0) as points_quarantined,
    round(100.0 * coalesce(pts.points_valid, 0)
        / nullif(coalesce(pts.points_valid, 0) + coalesce(q.points_quarantined, 0), 0), 2) as valid_pct,
    round(100.0 * coalesce(cover.seconds_with_data, 0) / 86400, 2) as coverage_pct,
    coalesce(q.q_missing_value, 0) as q_missing_value,
    coalesce(q.q_bad_timestamp, 0) as q_bad_timestamp,
    coalesce(q.q_future_timestamp, 0) as q_future_timestamp,
    coalesce(q.q_out_of_range, 0) as q_out_of_range,
    coalesce(q.q_unknown_sensor, 0) as q_unknown_sensor,
    coalesce(b.batches_loaded, 0) as batches_loaded,
    coalesce(b.duplicate_batches, 0) as duplicate_batches,
    coalesce(dupf.duplicate_files, 0) as duplicate_files,
    coalesce(b.late_batches, 0) as late_batches,
    round(b.lateness_p95_min, 1) as lateness_p95_min,
    coalesce(b.out_of_order_rows, 0) as out_of_order_rows,
    coalesce(b.unmapped_points, 0) as unmapped_points,
    coalesce(flat.flatlined_sensor_hours, 0) as flatlined_sensor_hours
from days
left join pts using (vessel_id, day)
left join cover using (vessel_id, day)
left join q using (vessel_id, day)
left join b using (vessel_id, day)
left join dupf using (vessel_id, day)
left join flat using (vessel_id, day)
