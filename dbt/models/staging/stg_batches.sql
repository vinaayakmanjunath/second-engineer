-- Latest processing record per file (a crashed run may have logged a file twice).
select * exclude (rn) from (
    select *, row_number() over (partition by file_name order by ingested_at desc) as rn
    from {{ source('silver', 'batches') }}
) where rn = 1
