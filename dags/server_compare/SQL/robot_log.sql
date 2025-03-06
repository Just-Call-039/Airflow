
select
       uniqueid,
       call_date,
       last_step,
       phone,
       REGEXP_SUBSTR(dialog, '[0-9]+') dilaog,
       REGEXP_SUBSTR(server_number, '[0-9]+') server_number,
       if (last_step in ('','0','1','111','261','262','361','362','371','372'), 1, 0) autootvetchik,
       client_status,
       assigned_user_id,
       directory,
       marker,
       billsec,
       if(real_billsec is not null, real_billsec, billsec) real_billsec,
       trunk_id,
       network_provider_c,
       city_c,
       town,
       region_c,
       ptv_c
  from suitecrm_robot.jc_robot_log
 where date(call_date) = '{date_i}'

 select
       dialog_id uniqueid,
       call_date,
       last_step,
       phone,
       robot_id dialog,
       server_number,
       if (last_step in ('','0','1','111','261','262','361','362','371','372'), 1, 0) autootvetchik,
       client_status,
       operator_id assigned_user_id,
       voice directory,
       marker,
       billsec,
       if(real_billsec is not null, real_billsec, billsec) real_billsec,
       trunk_id,
       network_provider network_provider_c,
       city city_c,
       town,
       quality region_c,
       ptv ptv_c
  from suitecrm_robot.robot_log 
       left join suitecrm_robot.robot_log_addition 
            on robot_log.id = robot_log_addition.robot_log_id
 where date(call_date) = '{date_i}'
