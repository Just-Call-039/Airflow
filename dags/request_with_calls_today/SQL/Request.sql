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
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_rostelecom as r
                           left join suitecrm.jc_meetings_rostelecom_cstm as r_c on r.id = r_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                  and status != 'change_flat'
                    and date(date_entered) >= '2023-05-01'
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
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_beeline as b
                           left join suitecrm.jc_meetings_beeline_cstm as b_c on b.id = b_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                  and status != 'change_flat'
                    and date(date_entered) >= '2023-05-01'
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
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_domru as d
                           left join suitecrm.jc_meetings_domru_cstm as d_c on d.id = d_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                  and status != 'change_flat'
                    and date(date_entered) >= '2023-05-01'
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
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_ttk as t
                           left join suitecrm.jc_meetings_ttk_cstm as t_c on t.id = t_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                  and status != 'change_flat'
                    and date(date_entered) >= '2023-05-01'
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
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_netbynet as n
                           left join suitecrm.jc_meetings_netbynet_cstm as n_c on n.id = n_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                  and status != 'change_flat'
                    and date(date_entered) >= '2023-05-01'
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
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_mts as m
                           left join suitecrm.jc_meetings_mts_cstm as m_c on m.id = m_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                  and status != 'change_flat'
                    and date(date_entered) >= '2023-05-01')



         select project,
                my_phone_work,
                request_date,
                user,
                super,
                status,
                last_queue_c
         from requests



