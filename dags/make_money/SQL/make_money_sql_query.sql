with temp_calls as (select cl_c.asterisk_caller_id_c as phone,
                           if((cl_c.queue_c in ('', ' ') or cl_c.queue_c is null), 'unknown_queue',
                              cl_c.queue_c)          as queue,
                           project_c,
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
                      and was_repeat = 1)

select temp_calls.phone, temp_calls.queue, temp_calls.project_c, temp_calls.date
from temp_calls
         inner join temp_robot on temp_calls.phone = temp_robot.phone;
