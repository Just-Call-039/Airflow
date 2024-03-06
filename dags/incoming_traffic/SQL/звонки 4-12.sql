with fios as (select id, concat(first_name, ' ', last_name) fio, team
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
     userrr as (SELECT distinct users.id,
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
                                    end                            team,
                                fios.fio                           supervisor
                FROM suitecrm.users
                         left join (select id_user, supervisor
                                    from (select id_user,
                                                 supervisor,
                                                 date(date_start),
                                                 row_number() over (partition by id_user order by date_start desc) rn
                                          from suitecrm.worktime_supervisor) R
                                    where rn = 1) worktime_supervisor on users.id = id_user
                         left join fios on supervisor = fios.id),

     operator as (select id, fio, replace(team, ' ', '') team, supervisor
                  from userrr),
     requests as (select 'RTK'                              as project,
                         if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as my_phone_work,
                         date(r.date_entered)               as request_date,
                         hour(r.date_entered) + 3              request_hour,
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
                    and date(date_entered) >= '2023-07-01'
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
                         hour(b.date_entered) + 3              request_hour,

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
                    and date(date_entered) >= '2023-07-01'
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
                         hour(d.date_entered) + 3              request_hour,

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
                    and date(date_entered) >= '2023-07-01'
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
                         hour(t.date_entered) + 3              request_hour,
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
                    and date(date_entered) >= '2023-07-01'
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
                         hour(n.date_entered) + 3              request_hour,

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
                    and date(date_entered) >= '2023-07-01'
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
                         hour(m.date_entered) + 3              request_hour,

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
                    and date(date_entered) >= '2023-07-01'
                  union all
                  select case
                             when project = 'tele2' then 'TELE2'
                             when project = 'selection' then 'Подбор'
                             when project = 'project_10' then 'проект10'
                             when project = 'project_9' then 'проект9'
                             when project = 'project_8' then 'GULFTSREAM'
                             when project = 'project_7' then 'проект10'
                             when project = 'project_6' then 'проект10'
                             when project = 'project_5' then 'проект10'
                             when project = 'project_4' then 'проект10'
                             when project = 'project_3' then 'проект10'
                             when project = 'project_2' then 'VKS'
                             when project = 'project_1' then 'TAT LIDS'
                             when project = 'bankruptcy' then 'BANK'
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
                         date(m.date_entered)               as request_date,
                         hour(m.date_entered) + 3              request_hour,

                         assigned_user_id                   as user,
                         user_id_c                          as super,
                         status,
                         last_queue_c,
                         'Остальные проекты'                   district_c
                  from suitecrm.jc_meetings_other as m
                           left join suitecrm.jc_meetings_other_cstm as m_c on m.id = m_c.id_c
                  where status != 'Error'
                    and status != 'doubled'
                    and status != 'change_flat'
                    and date(date_entered) >= '2023-07-01'),
     meets as (select project,
                      my_phone_work,
                      fio        user,
                      supervisor super,
                      last_queue_c,
                      status
               from requests
                        left join userrr on id = user),
     oper_calls as (select TT.*,
                           id contact_id,
                           town_c,
                           city_c
                    from (select if(calls.name = 'Входящий звонок', 1, 0)                                               name,
                                 direction,
                                 calls.assigned_user_id,
                                 calls.status,
                                 if(length(replace(
                                         replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''),
                                         ' ',
                                         '')) <=
                                    10,
                                    concat(8,
                                           replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''),
                                                           '(', ''), ' ', '')),
                                    concat(8,
                                           right(replace(replace(
                                                                 replace(replace(asterisk_caller_id_c, '-', ''), ')', ''),
                                                                 '(', ''), ' ',
                                                         ''),
                                                 10))) as                                                               asterisk_caller_id_c,
                                 result_call_c,
                                 otkaz_c,
                                 date(calls.date_entered)                                                               calldate,
                                 substring(dialog, 11, 4)                                                               dialog,
                                 duration_minutes,
                                 supervisor,
                                 row_number() over (partition by asterisk_caller_id_c order by calls.date_entered desc) row
                          from suitecrm.calls
                                   left join suitecrm.calls_cstm on id = id_c
                                   left join operator on assigned_user_id = operator.id
                                   left join suitecrm_robot.jc_robot_log
                                             on phone = asterisk_caller_id_c and
                                                date(call_date) = date(calls.date_entered)
                          where date(calls.date_entered) between '2023-10-01' and date(now()) - interval 1 day
                            and team not in ('12', '4')
                         ) TT
                             left join suitecrm.contacts
                                       on asterisk_caller_id_c = phone_work
                             left join suitecrm.contacts_cstm on id = id_c
                    where row = 1)

select TT.*,
       id                contact_id,
       contacts_cstm.town_c,
       contacts_cstm.city_c,
       supervisor,
       oper_calls.dialog dialog_one,
       super,
       user, my_phone_work,
       meets.status
from (select if(calls.name = 'Входящий звонок', 1, 0) name,
             direction,
             calls.assigned_user_id,
             if(length(replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ',
                               '')) <=
                10,
                concat(8,
                       replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ', '')),
                concat(8,
                       right(replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ',
                                     ''), 10))) as    asterisk_caller_id_c,
             result_call_c,
             otkaz_c,
             date(calls.date_entered)                 calldate,
             duration_minutes,
             queue_c
      from suitecrm.calls
               left join suitecrm.calls_cstm on id = id_c
               left join operator on assigned_user_id = operator.id
               left join suitecrm_robot.jc_robot_log
                         on phone = asterisk_caller_id_c and date(call_date) = date(calls.date_entered)
      where DATE_FORMAT(calls.date_entered, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
  AND calls.date_entered < DATE_SUB(CURDATE(), INTERVAL 0 DAY)
        and team in ('12', '4')
        and direction = 'I'
     ) TT
         left join suitecrm.contacts
                   on asterisk_caller_id_c = phone_work
         left join suitecrm.contacts_cstm on id = id_c
         left join oper_calls on TT.asterisk_caller_id_c = oper_calls.asterisk_caller_id_c
         left join meets on my_phone_work = TT.asterisk_caller_id_c