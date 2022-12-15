select distinct phone_number,
                assigned_user_id,
                status as status_request,
                date_reguest,
                uniqueid,
                ochered,
                project
from (select my_phone_work as phone_number,
             assigned_user_id,
             status,
             reguest.date  as date_reguest,
             my_date       as calls_date,
             new_rob.uniqueid,
             new_rob.ochered,
             new_rob.phone,
             project
      from (select 'RTK'                                                                   project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_rostelecom
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
            union all
            select 'Beeline'                                                               project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_beeline
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_domru
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_ttk
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
            union all
            select 'NBN'                                                                   project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_netbynet
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_mts jc_meetings_mts
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_beeline_mnp
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day) as reguest
               left join
           (select call_date + interval 2 hour as my_date,
                   uniqueid                    as uniqueid,
                   substring(dialog, 11, 4)    as ochered,
                   phone
            from suitecrm_robot.jc_robot_log as jrl
            where date(call_date) =
                  (select max(date(call_date)) from suitecrm_robot.jc_robot_log as jrl2 where jrl.phone = jrl2.phone)
              and phone in (select phone_work
                            from suitecrm.jc_meetings_rostelecom
                            where status != 'Error'
                              and date(date_entered) = date(now()) - interval 1 day
                            union all
                            select phone_work
                            from suitecrm.jc_meetings_beeline
                            where status != 'Error'
                              and date(date_entered) = date(now()) - interval 1 day
                            union all
                            select jc_meetings_domru.phone_work
                            from suitecrm.jc_meetings_domru
                            where status != 'Error'
                              and date(date_entered) = date(now()) - interval 1 day
                            union all
                            select jc_meetings_ttk.phone_work
                            from suitecrm.jc_meetings_ttk
                            where status != 'Error'
                              and date(date_entered) = date(now()) - interval 1 day
                            union all
                            select jc_meetings_netbynet.phone_work
                            from suitecrm.jc_meetings_netbynet
                            where status != 'Error'
                              and date(date_entered) = date(now()) - interval 1 day
                            union all
                            select jc_meetings_mts.phone_work
                            from suitecrm.jc_meetings_mts jc_meetings_mts
                            where status != 'Error'
                              and date(date_entered) = date(now()) - interval 1 day
                            union all
                            select jc_meetings_beeline_mnp.phone_work
                            from suitecrm.jc_meetings_beeline_mnp
                            where status != 'Error'
                              and date(date_entered) = date(now()) - interval 1 day)
              and date(call_date) <= date(now()) - interval 1 day
            group by phone) as new_rob
           on reguest.my_phone_work = new_rob.phone) as total;
