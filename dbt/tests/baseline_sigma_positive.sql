select * from {{ ref('mart_baselines') }} where sigma <= 0
