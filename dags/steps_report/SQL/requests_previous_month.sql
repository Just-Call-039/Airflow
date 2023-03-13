with request as (select 'RTK'                                 project,
                        if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                          '')) <=
                           10,
                           concat(8,
                                  replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                           concat(8,
                                  right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                ''), 10))) as my_phone_work,
                        date(date_entered)                 as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_rostelecom
                 where status != 'Error'
                   and month(date(date_entered)) = month(curdate() - interval 1 month)
                   and year(date(date_entered)) =
                       if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year), year(curdate()))
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
                        date(date_entered)                 as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_beeline
                 where status != 'Error'
                   and month(date(date_entered)) = month(curdate() - interval 1 month)
                   and year(date(date_entered)) =
                       if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year), year(curdate()))
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
                        date(date_entered)                 as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_domru
                 where status != 'Error'
                   and month(date(date_entered)) = month(curdate() - interval 1 month)
                   and year(date(date_entered)) =
                       if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year), year(curdate()))
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
                        date(date_entered)                 as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_ttk
                 where status != 'Error'
                   and month(date(date_entered)) = month(curdate() - interval 1 month)
                   and year(date(date_entered)) =
                       if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year), year(curdate()))
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
                        date(date_entered)                 as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_netbynet
                 where status != 'Error'
                   and month(date(date_entered)) = month(curdate() - interval 1 month)
                   and year(date(date_entered)) =
                       if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year), year(curdate()))
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
                        date(date_entered)                 as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_mts jc_meetings_mts
                 where status != 'Error'
                   and month(date(date_entered)) = month(curdate() - interval 1 month)
                   and year(date(date_entered)) =
                       if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year),
                          year(curdate()))),

     new_rob as (select call_date, uniqueid, ochered, phone
                 from (select date(call_date)                                                as call_date,
                              uniqueid,
                              substring(dialog, 11, 4)                                       as ochered,
                              phone,
                              row_number() over (partition by phone order by call_date desc) as num
                       from suitecrm_robot.jc_robot_log
                       where date(call_date) >= date(now()) - interval 120 day) as temp
                 where num = 1)

select my_phone_work as phone_number,
       assigned_user_id,
       status        as status_request,
       request.date  as date_request,
       new_rob.uniqueid,
       new_rob.ochered,
       project
from request
         left join new_rob on request.my_phone_work = new_rob.phone;
