select TT.*,
       id contact_id,
       town_c,
       city_c
from (select if(calls.name = 'Входящий звонок', 1, 0) name,
             direction,
             calls.assigned_user_id,
             calls.status,
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
             ne_reshena_c,
             reshena_c,
             date(calls.date_entered)                         calldate,
             dialog,
             duration_minutes
      from suitecrm.calls
               left join suitecrm.calls_cstm on id = id_c
               left join users on assigned_user_id = users.id
               left join 
                         (select phone,
                                call_date,
                                substring(dialog, 11, 4)                 dialog
                           from suitecrm_robot.jc_robot_log
                     union all 
                         select phone,
                                call_date,
                                robot_id                dialog
                           FROM  suitecrm_robot.robot_log 
                            left join suitecrm_robot.robot_log_addition 
                                   on robot_log.id = robot_log_addition.robot_log_id) rl
                         on phone = asterisk_caller_id_c and date(call_date) = date(calls.date_entered)
      where date(date_start) between '2023-09-01' and date(now()) - interval 1 day
        and (queue_c = 90003 or queue_c = 9018)
        and duration_minutes > 0
     ) TT
         left join suitecrm.contacts
                   on asterisk_caller_id_c = phone_work
left join suitecrm.contacts_cstm on id=id_c