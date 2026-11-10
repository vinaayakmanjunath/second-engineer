-- alerts are built only from running hours, so the first flagged hour must be a running hour
select a.* from {{ ref('mart_alerts') }} a
left join {{ ref('int_sensor_hourly') }} s
  on s.vessel_id = a.vessel_id and s.system = a.system and s.hour = a.first_flagged
where s.hour is null
