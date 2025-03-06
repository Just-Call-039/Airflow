select phone,
       call_date,
       dialog,
       last_step,
       contacts_status_c

from

(select phone,
        date(call_date) call_date,
        REGEXP_SUBSTR(dialog, '[0-9]+') dialog,
        last_step
       

   from suitecrm_robot.jc_robot_log
  where date(call_date) = curdate() - interval 1 day


union all

select phone,
       date(call_date) call_date,
       robot_id as dialog,
       last_step
      
   from suitecrm_robot.robot_log 
   left join suitecrm_robot.robot_log_addition 
        on robot_log.id = robot_log_addition.robot_log_id
 where date(call_date) = curdate() - interval 1 day
) as rl

left join suitecrm.contacts c
        on phone = c.phone_work
        left join suitecrm.contacts_cstm cc
                        on c.id = cc.id_c
where contacts_status_c = 'null_status' or contacts_status_c is null
