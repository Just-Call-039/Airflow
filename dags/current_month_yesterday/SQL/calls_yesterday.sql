with callsAll as (select distinct date(calls.date_entered) dateCall,
                         assigned_user_id       userid,
                         queue_c,
                         result_call_c,
                         otkaz_c,
                         project_c,
                         asterisk_caller_id_c,
                         duration_minutes,
                         concat(first_name,' ',last_name) fullname
                  from calls
                           left join calls_cstm on id = id_c
                           left join users on assigned_user_id = users.id
                  where direction = 'Inbound'
                    and DATE_FORMAT(calls.date_entered, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
  AND calls.date_entered < DATE_SUB(CURDATE(), INTERVAL 0 DAY)),
     robotlog as (select phone, city_c, assigned_user_id, call_date, substring(dialog, 11, 4) set_queue
                  from suitecrm_robot.jc_robot_log
                  where last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372') and (call_date) != day (curdate())
  and month (call_date) = month (curdate())
  and year (call_date) = year (curdate()))


select dateCall,
       userid,
       queue_c,
       result_call_c,
       otkaz_c,
       project_c,
       city_c,
       count(asterisk_caller_id_c),
       set_queue
from (
         select distinct dateCall,
                         userid,
                         queue_c,
                         result_call_c,
                         otkaz_c,
                         project_c,
                         city_c,
                         asterisk_caller_id_c,
                         duration_minutes,
                         set_queue
         from callsAll
                  left outer join robotlog
                                  on phone = asterisk_caller_id_c and date(dateCall) = date(call_date)) tg
group by dateCall,
         userid,
         queue_c,
         result_call_c,
         otkaz_c,
         project_c,
         city_c

