select distinct phone,
                date(call_date)                                                            calldate,
                DATE_FORMAT(DATE_ADD(call_date, INTERVAL IF(MINUTE(call_date) >= 58, 1, 0) HOUR),
                            '%H')                                                          hours,
                REGEXP_SUBSTR(dialog, '[0-9]+')                                                set_queue,
                last_step,
                if(last_step not in ('', '0', '111', '371', '372', '362', '361', '261', '262') and
                   client_status in ('refusing', 'MeetingWait', 'Wait', 'CallWait'), 1, 0) talk,
                if(client_status = 'MeetingWait', 1, 0)                                    meeting
from suitecrm_robot.jc_robot_log
where last_step not in ('', '0', '111', '371', '372', '362', '361', '261', '262')
  and date(call_date) = date(now()) - interval 1 day
  and (deleted = 0 or deleted is null)
  and (inbound_call = 0 or inbound_call is null)

union all

select distinct phone,
                date(call_date)                                                            calldate,
                DATE_FORMAT(DATE_ADD(call_date, INTERVAL IF(MINUTE(call_date) >= 58, 1, 0) HOUR),
                            '%H')                                                          hours,
                robot_id                                                   set_queue,
                last_step,
                if(last_step not in ('', '0', '111', '371', '372', '362', '361', '261', '262') and
                   client_status in ('refusing', 'MeetingWait', 'Wait', 'CallWait'), 1, 0) talk,
                if(client_status = 'MeetingWait', 1, 0)                                    meeting
from suitecrm_robot.robot_log 
         left join suitecrm_robot.robot_log_addition 
         on robot_log.id = robot_log_addition.robot_log_id
where last_step not in ('', '0', '111', '371', '372', '362', '361', '261', '262')
  and date(call_date) = date(now()) - interval 1 day
  and (direction = 0 or direction is null)