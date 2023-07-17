with callsAll as (select  date(calls.date_start) dateCall,
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
                    and month(date_start) = month(curdate() - interval 1 month)
                               and year(date_start) =
                                   if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year),
                                      year(curdate()))),
     robotlog as (select phone, city_c, assigned_user_id, call_date
                  from suitecrm_robot.jc_robot_log
                  where last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
         and month(call_date) = month(curdate() - interval 1 month)
                               and year(call_date) =
                                   if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year),
                                      year(curdate())))

select distinct dateCall,
       userid,
       queue_c,
       result_call_c,
       otkaz_c,
       project_c,
       asterisk_caller_id_c,
       city_c
from callsAll
         left outer join   robotlog
              on phone = asterisk_caller_id_c and date(dateCall) = date(call_date);

