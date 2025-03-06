with temp_calls as (select cl_c.asterisk_caller_id_c as caller_id,
                           if((cl_c.queue_c in ('', ' ') or cl_c.queue_c is null), 'unknown_queue',
                              cl_c.queue_c)          as queue_num_curr,
                           project_c as project_с,
                           date(date_entered) as date
                    from suitecrm.calls as cl
                             left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                    where direction = 'Inbound'
                      and date(date_entered)
                      -- between '2023-09-22' and '2023-09-23'
                      = date(now()) - interval 1 day
                      and result_call_c = 'refusing'
                      and otkaz_c in ('otkaz_23', 'otkaz_42', 'no_ansver')
                      and duration_minutes <= 10),

     temp_robot as (select distinct phone
                    from suitecrm_robot.jc_robot_log
                    where date(call_date) >= date(now()) - interval 2 month
                      and was_repeat = 1

                     union all 

                    select distinct phone 
                      from suitecrm_robot.robot_log 
                           left join suitecrm_robot.robot_log_addition 
                                on robot_log.id = robot_log_addition.robot_log_id
                     where date(call_date) >= date(now()) - interval 2 month
                           and was_ptv = 1)

select temp_calls.caller_id, temp_calls.queue_num_curr, temp_calls.project_с, temp_calls.date
from temp_calls
         inner join temp_robot on temp_calls.caller_id = temp_robot.phone;
