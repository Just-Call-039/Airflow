select assigned_user_id,
       contact_id_c,
       call_date,
       last_step,
       count_steps,
       uniqueid,
       client_status,
       otkaz,
       was_repeat,
       REGEXP_SUBSTR(dialog, '[0-9]+') queue,
       route,
       server_number,
       directory,
       billsec,
       town,
       inbound_call,
       marker,
       was_stepgroups,
       ptv_c,
       network_provider_c,
       city_c,
       region_c,
       phone
from suitecrm_robot.jc_robot_log
where date(call_date) = date(now()) - interval {n} day

union all

select operator_id assigned_user_id,
       contact_id contact_id_c,
       call_date,
       last_step,
       1 as count_steps,
       dialog_id uniqueid,
       client_status,
       refuse otkaz,
       was_ptv was_repeat,
       robot_id queue,
       route,
       server_number,
       voice directory,
       billsec,
       region as town,
       direction inbound_call,
       marker,
       0 was_stepgroups,
       ptv ptv_c,
       network_provider network_provider_c,
       city city_c,
       quality region_c,
       phone
from suitecrm_robot.robot_log 
     left join suitecrm_robot.robot_log_addition 
     on robot_log.id = robot_log_addition.robot_log_id
where date(call_date) = date(now()) - interval {n} day