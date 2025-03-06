with reguest as (select 'RTK'                                 project,
                        if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                          '')) <=
                           10,
                           concat(8,
                                  replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                           concat(8,
                                  right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                ''), 10))) as my_phone_work,
                        date_entered + interval 2 hour     as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_rostelecom
                 where status != 'Error'
                   and date(date_entered) = date(now()) - interval 1 day
                 union all
                 select 'Beeline'                             project,
                        if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                          '')) <=
                           10,
                           concat(8,
                                  replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                           concat(8,
                                  right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                ''), 10))) as my_phone_work,
                        date_entered + interval 2 hour     as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_beeline
                 where status != 'Error'
                   and date(date_entered) = date(now()) - interval 1 day
                 union all
                 select project,
                        if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                          '')) <=
                           10,
                           concat(8,
                                  replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                           concat(8,
                                  right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                ''), 10))) as my_phone_work,
                        date_entered + interval 2 hour     as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_domru
                 where status != 'Error'
                   and date(date_entered) = date(now()) - interval 1 day
                 union all
                 select project,
                        if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                          '')) <=
                           10,
                           concat(8,
                                  replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                           concat(8,
                                  right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                ''), 10))) as my_phone_work,
                        date_entered + interval 2 hour     as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_ttk
                 where status != 'Error'
                   and date(date_entered) = date(now()) - interval 1 day
                 union all
                 select 'NBN'                                 project,
                        if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                          '')) <=
                           10,
                           concat(8,
                                  replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                           concat(8,
                                  right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                ''), 10))) as my_phone_work,
                        date_entered + interval 2 hour     as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_netbynet
                 where status != 'Error'
                   and date(date_entered) = date(now()) - interval 1 day
                 union all
                 select project,
                        if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                          '')) <=
                           10,
                           concat(8,
                                  replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                           concat(8,
                                  right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                ''), 10))) as my_phone_work,
                        date_entered + interval 2 hour     as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_mts jc_meetings_mts
                 where status != 'Error'
                   and date(date_entered) = date(now()) - interval 1 day),

     new_rob as (select call_date, uniqueid, ochered, phone
                 from (select call_date + interval 2 hour                                    as call_date,
                              uniqueid,
                              substring(dialog, 11, 4)                                       as ochered,
                              phone,
                              row_number() over (partition by phone order by call_date desc) as num
                       from suitecrm_robot.jc_robot_log
                       where date(call_date) >= date(now()) - interval 120 day
                       union all
                       select call_date + interval 2 hour                                    as call_date,
                              dialog_id uniqueid,
                              robot_id                                       as ochered,
                              phone,
                              row_number() over (partition by phone order by call_date desc) as num
                       from suitecrm_robot.robot_log 
                            left join suitecrm_robot.robot_log_addition 
                            on robot_log.id = robot_log_addition.robot_log_id
                       where date(call_date) >= date(now()) - interval 120 day) as temp
                 where num = 1)

select my_phone_work as phone_number,
       assigned_user_id,
       status        as status_request,
       reguest.date  as date_reguest,
       new_rob.uniqueid,
       new_rob.ochered,
       project
from reguest
         left join new_rob on reguest.my_phone_work = new_rob.phone;
