with work_time as (select rc.id_user,
                          rc.date,
                          row_number() over (partition by rc.id_user, rc.date)                                               as num,
                          if(rc.talk_inbound is null, 0, rc.talk_inbound)                                                    as talk_inbound,
                          if(rc.talk_outbound is null, 0, rc.talk_outbound)                                                  as talk_outbound,
                          if((rc.fact - rc.talk_inbound -
                              if((rc.talk_outbound - (rc.recall - rc.recall_talk)) < 0, 0,
                                 (rc.talk_outbound - (rc.recall - rc.recall_talk))) -
                              rc.obrabotka_in_fact - rc.progul_obrabotka_in_fact) is null, 0, (rc.fact - talk_inbound -
                                                                                               if(
                                                                                                           (rc.talk_outbound - (rc.recall - rc.recall_talk)) <
                                                                                                           0,
                                                                                                           0,
                                                                                                           (rc.talk_outbound - (rc.recall - rc.recall_talk))) -
                                                                                               rc.obrabotka_in_fact -
                                                                                               rc.progul_obrabotka_in_fact)) as ozhidanie,
                          if(rc.obrabotka is null, 0, rc.obrabotka)                                                          as obrabotka,
                          if(rc.training is null, 0, rc.training)                                                            as training,
                          if(rc.nastavnik is null, 0, rc.nastavnik)                                                          as nastavnik,
                          if(rc.sobranie is null, 0, rc.sobranie)                                                            as sobranie,
                          if(rc.problems is null, 0, rc.problems)                                                            as problems,
                          if(rc.obuchenie is null, 0, rc.obuchenie)                                                          as obuchenie,
                          if(rc.recall is null, 0, rc.recall)                                                                as dorabotka,
                          if(rc.recall is null, 0, rc.recall_talk)                                                           as dorabotka_talk,
                          if(rc.pause10 is null, 0, rc.pause10)                                                              as pause,
                          if(timestampdiff(second, wt.lunch_start, wt.lunch_stop) is null, 0,
                             timestampdiff(second, wt.lunch_start, wt.lunch_stop))                                           as lunch_duration
                   from suitecrm.reports_cache as rc
                            left join suitecrm.worktime_log as wt
                                      on rc.id_user = wt.id_user and date(rc.date) = date(wt.date)
                   where rc.date between '{date_i}' and '{date_before}'
                     and rc.id_user not in ('1', '')
                     and rc.id_user is not null),

     timee as (select work_time.id_user,
                      work_time.date,
                      work_time.talk_inbound,
                      work_time.talk_outbound,
                      work_time.ozhidanie,
                      work_time.obrabotka,
                      work_time.training,
                      work_time.nastavnik,
                      work_time.sobranie,
                      work_time.problems,
                      work_time.obuchenie,
                      work_time.dorabotka,
                      work_time.pause,
                      work_time.lunch_duration,
                      work_time.dorabotka_talk,
                      (talk_inbound + talk_outbound + ozhidanie + obrabotka + training + nastavnik + sobranie +
                       problems +
                       obuchenie + dorabotka) / 3600 total_sec
               from work_time
               where work_time.num = 1
               order by work_time.date, work_time.id_user),


     calls as (select cl.id,
                      date(cl.date_entered)               as call_date,
                      DATE_FORMAT(DATE_ADD(cl.date_entered, INTERVAL IF(MINUTE(cl.date_entered) >= 58, 1, 0) HOUR),
                                  '%H')                      hours,
                      cl.name,
                      cl_c.asterisk_caller_id_c           as phone,
                      contacts.id                            contact_id,
                      if((cl_c.queue_c in ('', ' ') or cl_c.queue_c is null), 'unknown_queue',
                         cl_c.queue_c)                    as queue,
                      if((cl.assigned_user_id in ('', ' ') or cl.assigned_user_id is null), 'unknown_id',
                         cl.assigned_user_id)             as user_call,
                      if((cl_c.user_id_c in ('', ' ') or cl_c.user_id_c is null), 'unknown_id',
                         cl_c.user_id_c)                  as super,
                      case
                          when city_c is null then concat(town_c, '_t')
                          when city_c in ('', ' ') then concat(town_c, '_t')
                          else city_c
                          end                             as city,
                      duration_minutes                    as call_sec,
                      if(cl.duration_minutes <= 10, 1, 0) as short_calls,
                      completed_c
               from suitecrm.calls as cl
                        left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                        left join suitecrm.contacts on cl_c.asterisk_caller_id_c = contacts.phone_work
                        left join suitecrm.contacts_cstm on contacts_cstm.id_c = contacts.id
               where cl.date_entered between '{date_i}' and '{date_before}'),


     ws as (select *
            from (select *, row_number() over (partition by id_user order by date_start desc) as num
                  from suitecrm.worktime_supervisor) as temp
            where temp.num = 1),
     robot as (select phone, date(calldates) calldate, dialog, hours
               from (select *, row_number() over (partition by phone,date(calldates),hours order by calldates desc) row
                     from (select phone,
                                  call_date                calldates,
                                  dialog,
                                  DATE_FORMAT(DATE_ADD(call_date, INTERVAL IF(MINUTE(call_date) >= 58, 1, 0) HOUR),
                                              '%H')        hours
                           from (
                                       
                       
                                
                        SELECT 
                                phone,
                                call_date,
                                robot_id as dialog,
                                last_step
                                
                          FROM suitecrm_robot.robot_log 
                          left join suitecrm_robot.robot_log_addition 
                                on robot_log.id = robot_log_addition.robot_log_id
                        where last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
                                and call_date between '{date_i}' and '{date_before}'
                                        
                                ) as rl
                           ) yy) yyy
               where row = 1)

select distinct calls.id,
                calls.call_date,
                calls.name,
                if(contact_id is null, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                                                                                               REPLACE(calls.phone, '0', 'k'),
                                                                                                               '1',
                                                                                                               'a'),
                                                                                                       '2', 'z'),
                                                                                               '3', 'd'),
                                                                                       '4', 'e'),
                                                                               '5', 's'),
                                                                       '6', 'm'),
                                                               '7', 'h'),
                                                       '8', 'i'),
                                               '9', 'p'), contact_id)                                     contactid,
                calls.queue,
                calls.user_call,
                if((ws.supervisor in ('', ' ') or ws.supervisor is null), 'unknown_id', ws.supervisor) as super,
                calls.city,
                calls.call_sec,
                calls.short_calls,
                dialog,
                completed_c,
                total_sec/COUNT(*) OVER (PARTITION BY user_call, DATE(call_date)) AS call_count,
                calls.phone
from calls
         left join ws on calls.user_call = ws.id_user
         left join robot j on calls.phone = j.phone and call_date = calldate and j.hours = calls.hours
         left join timee on timee.id_user = user_call and date = call_date