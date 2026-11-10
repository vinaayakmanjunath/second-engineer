select * from {{ source('bronze', 'manifest') }}
