select call_date + interval 2 hour as my_date,
       uniqueid,
       substring(dialog, 11, 4)    as ochered,
       last_step,
       route,
       billsec,
       client_status,
       otkaz,
       directory,
       server_number,
       city_c,
       ptv_c,
       marker,
       was_repeat,
       phone
from suitecrm_robot.jc_robot_log
where (date(call_date) = '2022-04-18')
  and (timediff(time(now()), time(call_date)) between '03:15:00' and '03:24:59')
  
  union all

select call_date + interval 2 hour as my_date,
       dialog_id uniqueid,
       robot_id    as ochered,
       last_step,
       route,
       billsec,
       client_status,
       refuse otkaz,
       voice directory,
       server_number,
       city city_c,
       ptv ptv_c,
       marker,
       was_ptv was_repeat,
       phone
from suitecrm_robot.robot_log 
      left join suitecrm_robot.robot_log_addition 
      on robot_log.id = robot_log_addition.robot_log_id
where (date(call_date) = '2022-04-18')
  and (timediff(time(now()), time(call_date)) between '03:15:00' and '03:24:59')
  ;
