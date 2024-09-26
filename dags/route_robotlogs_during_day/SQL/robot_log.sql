SELECT phone,
       call_date as date,
       SUBSTRING(dialog, 11, 4) queue,
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
       was_repeat,
  CASE WHEN route = '' AND last_step = 1 THEN 1 ELSE route END AS route
  FROM suitecrm_robot.jc_robot_log
 WHERE date(call_date) = date(now())
 
--  CURRENT_DATE() - interval 1 day
 