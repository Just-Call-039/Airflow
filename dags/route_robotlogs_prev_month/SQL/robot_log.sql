
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
       marker,
       was_repeat,
  CASE WHEN route = '' AND last_step = 1 THEN 1 ELSE route END AS route,
  CASE WHEN network_provider_c = '83' THEN 'МТС'
       WHEN network_provider_c = '80' THEN 'Билайн'
       WHEN network_provider_c = '82' THEN 'Мегафон'
       WHEN network_provider_c IN ('68','10') THEN 'Теле2'
       ELSE 'MVNO'
       END AS network_provider
  FROM {file_name}
--  WHERE DATE_FORMAT(call_date, '%Y-%m') = DATE_FORMAT({i_month}, '%Y-%m')
       