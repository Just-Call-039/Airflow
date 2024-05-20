with callsAll as (select date(calls.date_start)             dateCall,
                         assigned_user_id                   userid,
                         queue_c,
                         result_call_c,
                         otkaz_c,
                         project_c,
                         asterisk_caller_id_c,
                         duration_minutes,
                         concat(first_name, ' ', last_name) fullname
                  from calls
                           left join calls_cstm on id = id_c
                           left join users on assigned_user_id = users.id
                  where direction = 'Inbound'
                    and month(date_start) = month(curdate() - interval 1 month)
                    and year(date_start) =
                        if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year),
                           year(curdate()))),
     robotlog as (select phone,
                         city_c,
                         assigned_user_id,
                         call_date,
                         substring(dialog, 11, 4)                                            set_queue,
                         marker,
                         if(ptv_c like '%^3^%'
                                or ptv_c like '%^5^%'
                                or ptv_c like '%^6^%'
                                or ptv_c like '%^10^%'
                                or ptv_c like '%^11^%'
                                or ptv_c like '%^19^%'
                                or ptv_c like '%^14^%', 'Наша разметка', 'Не наша разметка') ptv
                  from suitecrm_robot.jc_robot_log
                  where last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
                    and month(call_date) = month(curdate() - interval 1 month)
                    and year(call_date) =
                        if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year),
                           year(curdate())))

select dateCall,
       userid,
       queue_c,
       result_call_c,
       otkaz_c,
       project_c,
       city_c,
       count(asterisk_caller_id_c),
       set_queue,
       duration_minutes,
       marker,
       ptv

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
                         set_queue,
                         marker,
                         ptv
         from callsAll
                  left outer join robotlog
                                  on phone = asterisk_caller_id_c and date(dateCall) = date(call_date)) tg
group by dateCall,
         userid,
         queue_c,
         result_call_c,
         otkaz_c,
         project_c,
         city_c,
         set_queue,
         duration_minutes,
         marker,
         ptv