with calls as (select cl.id,
                      date(cl.date_entered)               as call_date,
                      DATE_FORMAT(DATE_ADD(cl.date_entered, INTERVAL IF(MINUTE(cl.date_entered) >= 58, 1, 0) HOUR),
                                  '%H')                      hours,
                      cl.name,
                      cl_c.asterisk_caller_id_c           as phone,
                      if((cl_c.queue_c in ('', ' ') or cl_c.queue_c is null), 'unknown_queue',
                         cl_c.queue_c)                    as queue,
                      if((cl.assigned_user_id in ('', ' ') or cl.assigned_user_id is null), 'unknown_id',
                         cl.assigned_user_id)             as user_call,
                      if((cl_c.user_id_c in ('', ' ') or cl_c.user_id_c is null), 'unknown_id',
                         cl_c.user_id_c)                  as super,
                      case
                          when city_c is null then concat(town_c, 'OBL')
                          when city_c in ('', ' ') then concat(town_c, 'OBL')
                          else city_c
                          end                             as city,
                      duration_minutes                    as call_sec,
                      if(cl.duration_minutes <= 10, 1, 0) as short_calls
               from suitecrm.calls as cl
                        left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                        left join suitecrm.contacts on cl_c.asterisk_caller_id_c = contacts.phone_work
                        left join suitecrm.contacts_cstm on contacts_cstm.id_c = contacts.id
               where (month(cl.date_entered) = month(curdate() - interval 1 month)
                               and year(cl.date_entered) =
                                   if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year),
                                      year(curdate())))),

     ws as (select *
            from (select *, row_number() over (partition by id_user order by date_start desc) as num
                  from suitecrm.worktime_supervisor) as temp
            where temp.num = 1),
     robot as (select phone, date(calldates) calldate, dialog, hours
               from (select *, row_number() over (partition by phone,date(calldates),hours order by calldates desc) row
                     from (select phone,
                                  call_date                calldates,
                                  substring(dialog, 11, 4) dialog,
                                  DATE_FORMAT(DATE_ADD(call_date, INTERVAL IF(MINUTE(call_date) >= 58, 1, 0) HOUR),
                                              '%H')        hours
                           from suitecrm_robot.jc_robot_log
                           where last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
                             and (month(call_date) = month(curdate() - interval 1 month)
                               and year(call_date) =
                                   if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year),
                                      year(curdate())))) yy) yyy
               where row = 1)


select distinct calls.id,
                calls.call_date,
                calls.name,
                calls.phone,
                calls.queue,
                calls.user_call,
                if((ws.supervisor in ('', ' ') or ws.supervisor is null), 'unknown_id', ws.supervisor) as super,
                calls.city,
                calls.call_sec,
                calls.short_calls,
                dialog
from calls
         left join ws on calls.user_call = ws.id_user
         left join robot j on calls.phone = j.phone and call_date = calldate and j.hours = calls.hours
