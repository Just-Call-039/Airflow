SELECT phone,
       call_date as date,
       REGEXP_SUBSTR(dialog, '[0-9]+') queue,
       last_step,
       inbound_call,       
       client_status,
       real_billsec,
       trunk_id,
       assigned_user_id,
       count_steps,
       billsec,
       ptv_c,
       city_c,
       region_c,
       directory,
       marker,
       was_repeat,
  CASE WHEN route = '' AND last_step = 1 THEN 1 ELSE route END AS route,
  CASE WHEN network_provider_c = '83' THEN 'МТС'
       WHEN network_provider_c = '80' THEN 'Билайн'
       WHEN network_provider_c = '82' THEN 'Мегафон'
       WHEN network_provider_c IN ('68','10') THEN 'Теле2'
       ELSE 'MVNO'
       END AS network_provider
  FROM suitecrm_robot.jc_robot_log
 WHERE date(call_date) = date(now()) - interval {n} day

 UNION ALL

 SELECT phone,
       call_date as date,
       robot_id queue,
       last_step,
       direction inbound_call,       
       client_status,
       real_billsec,
       trunk_id,
       operator_id assigned_user_id,
       1 count_steps,
       billsec,
       ptv ptv_c,
       city city_c,
       quality region_c,
       voice directory,
       marker,
       was_ptv was_repeat,
  CASE WHEN route = '' AND last_step = 1 THEN 1 ELSE route END AS route,
  CASE WHEN network_provider = '83' THEN 'МТС'
       WHEN network_provider = '80' THEN 'Билайн'
       WHEN network_provider = '82' THEN 'Мегафон'
       WHEN network_provider IN ('68','10') THEN 'Теле2'
       ELSE 'MVNO'
       END AS network_provider
  FROM suitecrm_robot.robot_log 
       LEFT JOIN suitecrm_robot.robot_log_addition 
            ON robot_log.id = robot_log_addition.robot_log_id
 WHERE date(call_date) = date(now()) - interval {n} day


-- SELECT phone,
--        call_date as date,
--        SUBSTRING(dialog, 11, 4) queue,
--        last_step,
--        inbound_call,
--        client_status,
--        real_billsec,
--        trunk_id,
--        assigned_user_id,
--        count_steps,
--        billsec,
--        ptv_c,
--        city_c,
--        region_c,
--        directory,
--        was_repeat,
--   CASE WHEN route = '' AND last_step = 1 THEN 1 ELSE route END AS route
--   FROM suitecrm_robot.jc_robot_log_2024_06
--  WHERE date(call_date) = "2024-06-02" - interval {n} day
 
-- --  CURRENT_DATE() - interval 1 day
 