select
       uniqueid,
       call_date,
       last_step,
       phone,
       SUBSTRING(dialog, 11, 4) dialog,
       server_number,
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
 where date(call_date) = '{}'

 