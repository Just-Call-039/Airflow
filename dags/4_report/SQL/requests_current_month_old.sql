with requests as (select 'RTK'                              as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as my_phone_work,
                         date(r.date_entered)               as request_date,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status
                  from suitecrm.jc_meetings_rostelecom as r
                           left join suitecrm.jc_meetings_rostelecom_cstm as r_c on r.id = r_c.id_c
                  where status != 'Error'
                    and day(date(date_entered)) != day(curdate())
                    and month(date(date_entered)) = month(curdate())
                    and year(date(date_entered)) = year(curdate())
                  union all
                  select 'Beeline'                          as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as my_phone_work,
                         date(b.date_entered)               as request_date,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status
                  from suitecrm.jc_meetings_beeline as b
                           left join suitecrm.jc_meetings_beeline_cstm as b_c on b.id = b_c.id_c
                  where status != 'Error'
                    and day(date(date_entered)) != day(curdate())
                    and month(date(date_entered)) = month(curdate())
                    and year(date(date_entered)) = year(curdate())
                  union all
                  select 'DOMRU'                            as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as my_phone_work,
                         date(d.date_entered)               as request_date,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status
                  from suitecrm.jc_meetings_domru as d
                           left join suitecrm.jc_meetings_domru_cstm as d_c on d.id = d_c.id_c
                  where status != 'Error'
                    and day(date(date_entered)) != day(curdate())
                    and month(date(date_entered)) = month(curdate())
                    and year(date(date_entered)) = year(curdate())
                  union all
                  select 'TTK'                              as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as my_phone_work,
                         date(t.date_entered)               as request_date,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status
                  from suitecrm.jc_meetings_ttk as t
                           left join suitecrm.jc_meetings_ttk_cstm as t_c on t.id = t_c.id_c
                  where status != 'Error'
                    and day(date(date_entered)) != day(curdate())
                    and month(date(date_entered)) = month(curdate())
                    and year(date(date_entered)) = year(curdate())
                  union all
                  select 'NBN'                              as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as my_phone_work,
                         date(n.date_entered)               as request_date,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status
                  from suitecrm.jc_meetings_netbynet as n
                           left join suitecrm.jc_meetings_netbynet_cstm as n_c on n.id = n_c.id_c
                  where status != 'Error'
                    and day(date(date_entered)) != day(curdate())
                    and month(date(date_entered)) = month(curdate())
                    and year(date(date_entered)) = year(curdate())
                  union all
                  select 'MTS'                              as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as my_phone_work,
                         date(m.date_entered)               as request_date,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status
                  from suitecrm.jc_meetings_mts as m
                           left join suitecrm.jc_meetings_mts_cstm as m_c on m.id = m_c.id_c
                  where status != 'Error'
                    and day(date(date_entered)) != day(curdate())
                    and month(date(date_entered)) = month(curdate())
                    and year(date(date_entered)) = year(curdate())),

     calls as (select *
               from (select cl.id                                                                               as id_call,
                            cl_c.asterisk_caller_id_c                                                           as phone_number,
                            date(cl.date_entered)                                                               as call_date,
                            cl_c.result_call_c,
                            row_number() over (partition by cl_c.asterisk_caller_id_c order by cl.date_entered) as num
                     from suitecrm.calls as cl
                              left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                     where (day(date(cl.date_entered)) != day(curdate())
                         and month(date(cl.date_entered)) = month(curdate())
                         and year(date(cl.date_entered)) = year(curdate()))) as temp
               where num = 1)

select *
from requests
         left join calls on requests.my_phone_work = calls.phone_number;
