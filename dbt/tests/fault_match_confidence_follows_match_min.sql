-- A match is confident exactly when its similarity reaches match_min; similarity is a cosine;
-- only the top match carries a margin over the runner-up.
select * from {{ ref('mart_fault_match') }}
where confident is distinct from coalesce(similarity >= {{ var('match_min') }}, false)
   or similarity not between -1 and 1
   or (rank > 1 and margin is not null)
