-- Which bench fault does each alert look like? Cosine similarity between the alert's median
-- relative deviation per measurement and each fault signature. Top 3 per alert.
with alert_vec as (
    select a.alert_id, z.measurement, median(z.rel_dev) as rel_dev
    from {{ ref('mart_alerts') }} a
    join {{ ref('mart_anomaly_scores') }} z
      on z.vessel_id = a.vessel_id and z.system = a.system
     and z.hour >= a.first_flagged and z.hour < a.last_seen
    where z.measurement in (select distinct measurement from {{ ref('fault_signatures') }})
    group by all
),
pairs as (
    select v.alert_id, f.fault,
        sum(v.rel_dev * f.rel_dev)
            / nullif(sqrt(sum(v.rel_dev * v.rel_dev)) * sqrt(sum(f.rel_dev * f.rel_dev)), 0) as similarity
    from alert_vec v join {{ ref('fault_signatures') }} f using (measurement)
    group by all
)
select alert_id, fault, round(similarity, 3) as similarity,
    row_number() over (partition by alert_id order by similarity desc nulls last) as rank
from pairs
qualify rank <= 3
