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
                 from suitecrm.jc_meetings_beeline_mnp
                 where status != 'Error'
                   and date(date_entered) = date(now()) - interval 1 day),

     new_rob as (select call_date + interval 2 hour as my_date, uniqueid, substring(dialog, 11, 4) as ochered, phone
                 from suitecrm_robot.jc_robot_log
                 where date(call_date) >= date(now()) - interval 120 day)

select phone_number,
       assigned_user_id,
       status as status_request,
       date_reguest,
       uniqueid,
       ochered,
       project
from (select my_phone_work                                                as phone_number,
             assigned_user_id,
             status,
             reguest.date                                                 as date_reguest,
             new_rob.uniqueid,
             new_rob.ochered,
             new_rob.phone,
             row_number() over (partition by phone order by my_date desc) as num,
             project
      from reguest
               left join new_rob on reguest.my_phone_work = new_rob.phone) as total
where num = 1;
