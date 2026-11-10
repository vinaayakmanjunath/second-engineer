-- every batch the silver log saw must be counted in the scorecard (loaded and duplicate alike)
with dq as (select sum(batches_loaded) as loaded, sum(duplicate_batches) as dup from {{ ref('mart_dq_daily') }}),
log as (
    select count(*) filter (where status = 'loaded') as loaded,
           count(*) filter (where status = 'duplicate_batch') as dup
    from {{ ref('stg_batches') }}
)
select * from dq, log where dq.loaded <> log.loaded or dq.dup <> log.dup
