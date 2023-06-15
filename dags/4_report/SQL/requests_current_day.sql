with requests as (select 'RTK'                              as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as my_phone_work,
                         r.date_entered + interval 2 hour   as request_date,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_rostelecom as r
                           left join suitecrm.jc_meetings_rostelecom_cstm as r_c on r.id = r_c.id_c
                  where status != 'Error'
                    and date(date_entered) = date(curdate())
                    and hour(time(date_entered + interval 2 hour)) < hour(time(now() - interval 1 hour))
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
                         b.date_entered + interval 2 hour   as request_date,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_beeline as b
                           left join suitecrm.jc_meetings_beeline_cstm as b_c on b.id = b_c.id_c
                  where status != 'Error'
                    and date(date_entered) = date(curdate())
                    and hour(time(date_entered + interval 2 hour)) < hour(time(now() - interval 1 hour))
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
                         d.date_entered + interval 2 hour   as request_date,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_domru as d
                           left join suitecrm.jc_meetings_domru_cstm as d_c on d.id = d_c.id_c
                  where status != 'Error'
                    and date(date_entered) = date(curdate())
                    and hour(time(date_entered + interval 2 hour)) < hour(time(now() - interval 1 hour))
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
                         t.date_entered + interval 2 hour   as request_date,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_ttk as t
                           left join suitecrm.jc_meetings_ttk_cstm as t_c on t.id = t_c.id_c
                  where status != 'Error'
                    and date(date_entered) = date(curdate())
                    and hour(time(date_entered + interval 2 hour)) < hour(time(now() - interval 1 hour))
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
                         n.date_entered + interval 2 hour   as request_date,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_netbynet as n
                           left join suitecrm.jc_meetings_netbynet_cstm as n_c on n.id = n_c.id_c
                  where status != 'Error'
                    and date(date_entered) = date(curdate())
                    and hour(time(date_entered + interval 2 hour)) < hour(time(now() - interval 1 hour))
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
                         m.date_entered + interval 2 hour   as request_date,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_mts as m
                           left join suitecrm.jc_meetings_mts_cstm as m_c on m.id = m_c.id_c
                  where status != 'Error'
                    and date(date_entered) = date(curdate())
                    and hour(time(date_entered + interval 2 hour)) < hour(time(now() - interval 1 hour))),

     calls as (select *
               from (select cl.id                                                                                    as id_call,
                            cl_c.asterisk_caller_id_c                                                                as phone_number,
                            cl.date_entered                                                                          as call_date,
                            cl_c.result_call_c,
                            case
                                when city_c is null then concat(town_c, 'OBL')
                                when city_c in ('', ' ') then concat(town_c, 'OBL')
                                else city_c
                                end                                                                                  as city,
                            row_number() over (partition by cl_c.asterisk_caller_id_c order by cl.date_entered desc) as num
                     from suitecrm.calls as cl
                              left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                              left join suitecrm.contacts on cl_c.asterisk_caller_id_c = contacts.phone_work
                              left join suitecrm.contacts_cstm on contacts_cstm.id_c = contacts.id
                     where (date(now()) - interval 90 day <= date(cl.date_entered))) as temp
               where num = 1)

select project,my_phone_work,request_date,user,super,status,last_queue_c,id_call,phone_number,call_date,result_call_c,city,num
from requests
         left join calls on requests.my_phone_work = calls.phone_number;
