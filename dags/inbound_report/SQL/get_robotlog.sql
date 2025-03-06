select
      phone,
      (call_date + interval 3 hour) date,
      userid,
      last_step,
      client_status,
      billsec_r,
      queue_r

from 
(select 
      if(phone = 'anonymous', '80000000000', phone) phone,
      call_date,
      assigned_user_id as userid,
      last_step,
      client_status,
      billsec billsec_r,
      REGEXP_SUBSTR(dialog, '[0-9]+') queue_r
from suitecrm_robot.jc_robot_log

where date(call_date) BETWEEN '{start}' AND '{end}'
and route like '%%262%%'
and deleted = 0

union all 

select

      if(phone = 'anonymous', '80000000000', phone) phone,
      call_date,
      operator_id      as        userid,
      last_step,
      client_status,
      billsec billsec_r,
      robot_id queue_r
FROM  suitecrm_robot.robot_log 
      left join suitecrm_robot.robot_log_addition 
      on robot_log.id = robot_log_addition.robot_log_id

where date(call_date) BETWEEN '{start}' AND '{end}'
and route like '%%262%%' ) as rl