-- sigma_source says which term set sigma: the robust MAD estimate or the floor (sigma_floor_frac x median).
select * from {{ ref('mart_baselines') }}
where sigma_source not in ('mad', 'floor')
   or (sigma_source = 'floor') is distinct from (robust_sigma < {{ var('sigma_floor_frac') }} * abs(median_value))
