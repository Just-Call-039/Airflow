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
      from (select 'RTK'                                                                   project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_rostelecom
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
              and (timediff(time(now()), time(date_entered)) between '03:15:00' and '03:24:59')
            union all
            select 'Beeline'                                                               project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_beeline
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
              and (timediff(time(now()), time(date_entered)) between '03:15:00' and '03:24:59')
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_domru
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
              and (timediff(time(now()), time(date_entered)) between '03:15:00' and '03:24:59')
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_ttk
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
              and (timediff(time(now()), time(date_entered)) between '03:15:00' and '03:24:59')
            union all
            select 'NBN'                                                                   project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_netbynet
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
              and (timediff(time(now()), time(date_entered)) between '03:15:00' and '03:24:59')
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_mts jc_meetings_mts
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
              and (timediff(time(now()), time(date_entered)) between '03:15:00' and '03:24:59')
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_beeline_mnp
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
              and (timediff(time(now()), time(date_entered)) between '03:15:00' and '03:24:59')) as reguest
               left join
           (select call_date + interval 2 hour as my_date,
                   uniqueid,
                   substring(dialog, 11, 4)    as ochered,
                   phone
            from suitecrm_robot.jc_robot_log
            where date(call_date) >= date(now()) - interval 90 day
            union all
            select call_date + interval 2 hour as my_date,
                   dialog_id uniqueid,
                   robot_id    as ochered,
                   phone
            from suitecrm_robot.robot_log 
                     left join suitecrm_robot.robot_log_addition 
                     on robot_log.id = robot_log_addition.robot_log_id
            where date(call_date) >= date(now()) - interval 90 day) as new_rob
           on reguest.my_phone_work = new_rob.phone) as total
where num = 1;
