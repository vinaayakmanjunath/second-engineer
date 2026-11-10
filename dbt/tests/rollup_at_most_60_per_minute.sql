select * from {{ ref('mart_rollup_1min') }} where n > 60
