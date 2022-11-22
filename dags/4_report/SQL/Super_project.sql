with phone_number as (select *
                      from (select cl.id,
                                   cl_c.asterisk_caller_id_c,
                                   date(cl.date_entered)                                                            as call_date,
                                   if((cl_c.user_id_c in ('', ' ') or cl_c.user_id_c is null), 'unknown_id',
                                      cl_c.user_id_c)                                                               as super,
                                   row_number() over (partition by asterisk_caller_id_c order by date_entered desc) as num
                            from suitecrm.calls as cl
                                     left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                            where date(date_entered) between date(now()) - interval 30 day and date(now()) - interval 1 day) as temp
                      where num = 1),
     request as (select 'RTK'                                                                project,
                        concat(8,
                               right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                        date(date_entered)                                                as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_rostelecom
                 where status != 'Error'
                   and date(date_entered) between date(now()) - interval 30 day and date(now()) - interval 1 day
                 union all
                 select 'Beeline'                                                            project,
                        concat(8,
                               right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                        date(date_entered)                                                as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_beeline
                 where status != 'Error'
                   and date(date_entered) between date(now()) - interval 30 day and date(now()) - interval 1 day
                 union all
                 select 'DOMRU'                                                              project,
                        concat(8,
                               right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                        date(date_entered)                                                as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_domru
                 where status != 'Error'
                   and date(date_entered) between date(now()) - interval 30 day and date(now()) - interval 1 day
                 union all
                 select 'TTK'                                                                project,
                        concat(8,
                               right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                        date(date_entered)                                                as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_ttk
                 where status != 'Error'
                   and date(date_entered) between date(now()) - interval 30 day and date(now()) - interval 1 day
                 union all
                 select 'NBN'                                                                project,
                        concat(8,
                               right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                        date(date_entered)                                                as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_netbynet
                 where status != 'Error'
                   and date(date_entered) between date(now()) - interval 30 day and date(now()) - interval 1 day
                 union all
                 select 'MTS'                                                                project,
                        concat(8,
                               right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                        date(date_entered)                                                as date,
                        assigned_user_id,
                        status
                 from suitecrm.jc_meetings_mts jc_meetings_mts
                 where status != 'Error'
                   and date(date_entered) between date(now()) - interval 30 day and date(now()) - interval 1 day)
select distinct phone_number.super,
       request.project
from request
         left join phone_number on phone_number.asterisk_caller_id_c =
                                   request.my_phone_work and
                                   date(phone_number.call_date) =
                                   date(request.date)
where phone_number.super is not null;
