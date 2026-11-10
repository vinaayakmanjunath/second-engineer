-- Latest processing of each file only (a crash before the manifest update re-processes files;
-- same rule as silver.read_log and stg_batches).
select * from {{ source('silver', 'quarantine') }}
qualify ingested_at = max(ingested_at) over (partition by file_name)
