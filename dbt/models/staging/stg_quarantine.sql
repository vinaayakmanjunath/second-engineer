select * from {{ source('silver', 'quarantine') }}
