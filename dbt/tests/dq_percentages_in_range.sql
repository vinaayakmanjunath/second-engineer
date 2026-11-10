select * from {{ ref('mart_dq_daily') }}
where valid_pct not between 0 and 100 or coverage_pct not between 0 and 100
