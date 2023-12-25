with requests as (select 'RTK'                              as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as phone_request,
                         date(r.date_entered)               as request_date,
                         date(r.date_start)                    installation_date,
                         assigned_user_id                   as main_user,
                         created_by                            create_user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_rostelecom as r
                           left join suitecrm.jc_meetings_rostelecom_cstm as r_c on r.id = r_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
                  union all
                  select 'Beeline'                          as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as phone_request,
                         date(b.date_entered)               as request_date,
                         date(b.date_start)                    installation_date,
                         assigned_user_id                   as main_user,
                         created_by                            create_user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_beeline as b
                           left join suitecrm.jc_meetings_beeline_cstm as b_c on b.id = b_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
                  union all
                  select 'DOMRU'                            as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as phone_request,
                         date(d.date_entered)               as request_date,
                         date(d.date_start)                    installation_date,
                         assigned_user_id                   as main_user,
                         created_by                            create_user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_domru as d
                           left join suitecrm.jc_meetings_domru_cstm as d_c on d.id = d_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
                  union all
                  select 'TTK'                              as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as phone_request,
                         date(t.date_entered)               as request_date,
                         date(t.date_start)                    installation_date,
                         assigned_user_id                   as main_user,
                         created_by                            create_user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_ttk as t
                           left join suitecrm.jc_meetings_ttk_cstm as t_c on t.id = t_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
                  union all
                  select 'NBN'                              as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as phone_request,
                         date(n.date_entered)               as request_date,
                         date(n.date_start)                    installation_date,
                         assigned_user_id                   as main_user,
                         created_by                            create_user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_netbynet as n
                           left join suitecrm.jc_meetings_netbynet_cstm as n_c on n.id = n_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
                  union all
                  select 'MTS'                              as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as phone_request,
                         date(m.date_entered)               as request_date,
                         date(m.date_start)                    installation_date,
                         assigned_user_id                   as main_user,
                         created_by                            create_user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_mts as m
                           left join suitecrm.jc_meetings_mts_cstm as m_c on m.id = m_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
                  union all
                  select 'Other'                            as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as phone_request,
                         date(m.date_entered)               as request_date,
                         date(m.date_start)                    installation_date,
                         assigned_user_id                   as main_user,
                         created_by                            create_user,
                         user_id_c                          as super,
                         status,
                         last_queue_c
                  from suitecrm.jc_meetings_other as m
                           left join suitecrm.jc_meetings_other_cstm as m_c on m.id = m_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)),

     fio as (select id, concat(first_name, ' ', last_name) fio, team
             from (select id,
                          first_name,
                          last_name,
                          department_c,
                          case
                              when substring_index(substring_index(first_name, ' ', 3), ' ', -1) REGEXP '^[0-9]+$'
                                  then substring_index(substring_index(first_name, ' ', 3), ' ', -1)
                              when substring_index(substring_index(first_name, ' ', 4), ' ', -1) REGEXP '^[0-9]+$'
                                  then substring_index(substring_index(first_name, ' ', 4), ' ', -1)
                              else
                                  (case
                                       when left(first_name, instr(first_name, ' ') - 1) > 0 and
                                            left(first_name, instr(first_name, ' ') - 1) < 10000
                                           then left(first_name, instr(first_name, ' ') - 1)
                                       when left(first_name, 2) = '�_'
                                           then substring(first_name, 3, (instr(first_name, ' ') - 3))
                                       when left(first_name, 1) = '�'
                                           then substring(first_name, 2, (instr(first_name, ' ') - 1))
                                       else '' end)
                              end team
                   from suitecrm.users
                            left join suitecrm.users_cstm on users.id = users_cstm.id_c
                   where id in (select distinct supervisor from suitecrm.worktime_supervisor)) R1),
     userrr as (SELECT distinct users.id                           userid,
                                concat(first_name, ' ', last_name) fio,
                                case
                                    when substring_index(substring_index(first_name, ' ', 3), ' ', -1) REGEXP '^[0-9]+$'
                                        then substring_index(substring_index(first_name, ' ', 3), ' ', -1)
                                    when substring_index(substring_index(first_name, ' ', 4), ' ', -1) REGEXP '^[0-9]+$'
                                        then substring_index(substring_index(first_name, ' ', 4), ' ', -1)
                                    else
                                        (case
                                             when left(first_name, instr(first_name, ' ') - 1) > 0 and
                                                  left(first_name, instr(first_name, ' ') - 1) < 10000
                                                 then left(first_name, instr(first_name, ' ') - 1)
                                             when left(first_name, 2) = '�_'
                                                 then substring(first_name, 3, (instr(first_name, ' ') - 3))
                                             when left(first_name, 1) = '�'
                                                 then substring(first_name, 2, (instr(first_name, ' ') - 1))
                                             else '' end)
                                    end                            team
                FROM suitecrm.users)


select project,
       phone_request,
       request_date,
       installation_date,
       concat(first_name, ' ', last_name) main_user,
       userrr.fio                         create_user,
       fio.fio                            super,
       fio.team,
       requests.status,
       last_queue_c
from requests
         left join users on id = main_user
         left join userrr on userid = create_user
         left join fio on fio.id = super;
