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
                         hour(r.date_entered) + 2              request_hour,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c,
                         'Остальные проекты'                   district_c
                  from suitecrm.jc_meetings_rostelecom as r
                           left join suitecrm.jc_meetings_rostelecom_cstm as r_c on r.id = r_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= '2023-12-01'
                  union all
                  select 'BEELINE'                          as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as my_phone_work,
                         date(b.date_entered)               as request_date,
                         hour(b.date_entered) + 2              request_hour,

                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c,
                         'Остальные проекты'                   district_c
                  from suitecrm.jc_meetings_beeline as b
                           left join suitecrm.jc_meetings_beeline_cstm as b_c on b.id = b_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= '2023-12-01'
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
                         hour(d.date_entered) + 2              request_hour,

                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c,
                         'Остальные проекты'                   district_c
                  from suitecrm.jc_meetings_domru as d
                           left join suitecrm.jc_meetings_domru_cstm as d_c on d.id = d_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= '2023-12-01'
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
                         hour(t.date_entered) + 2              request_hour,
                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c,
                         'Остальные проекты'                   district_c
                  from suitecrm.jc_meetings_ttk as t
                           left join suitecrm.jc_meetings_ttk_cstm as t_c on t.id = t_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= '2023-12-01'
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
                         hour(n.date_entered) + 2              request_hour,

                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c,
                         'Остальные проекты'                   district_c
                  from suitecrm.jc_meetings_netbynet as n
                           left join suitecrm.jc_meetings_netbynet_cstm as n_c on n.id = n_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= '2023-12-01'
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
                         hour(m.date_entered) + 2              request_hour,

                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c,
                         case
                             when district_c = 'MTS_regions' then 'МТС Регионы'
                             when district_c = 'MTS_Moscow' then 'МТС Москва'
                             else 'МТС пусто' end              district_c
                  from suitecrm.jc_meetings_mts as m
                           left join suitecrm.jc_meetings_mts_cstm as m_c on m.id = m_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= '2023-12-01'
                  union all
                  select case
                             when project = 'tele2' then 'TELE2'
                             when project = 'selection' then 'Подбор'
                             when project = 'project_10' then 'проект10'
                             when project = 'project_9' then 'проект9'
                             when project = 'project_8' then 'GULFSTREAM'
                             when project = 'project_7' then 'проект7'
                             when project = 'project_6' then 'проект6'
                             when project = 'project_5' then 'проект5'
                             when project = 'project_4' then 'проект4'
                             when project = 'project_3' then 'проект3'
                             when project = 'project_2' then 'ВСК Страхование'
                             when project = 'project_1' then 'TATTELEKOM'
                             when project = 'bankruptcy' then 'Банкротство'
                             when project = 'hr' then 'HR'
                             else '' end                    as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as my_phone_work,
                         date(o.date_entered)               as request_date,
                         hour(o.date_entered) + 2              request_hour,

                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c,
                         'Остальные проекты'                   district_c
                  from suitecrm.jc_meetings_other as o
                           left join suitecrm.jc_meetings_other_cstm as o_c on o.id = o_c.id_c
                      and date(o.date_entered) >= '2023-12-01'
                  where status != 'Error'
                    and status != 'doubled')

select project,
       my_phone_work,
       request_date,
       if(request_hour=25, 24, request_hour)request_hour,
       user,
       super,
       status,
       last_queue_c,
       district_c
from requests
WHERE request_date >='2023-12-01'



