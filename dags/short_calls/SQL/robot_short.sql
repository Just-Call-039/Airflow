select distinct phone,
                date(call_date)                                                            calldate,
                DATE_FORMAT(DATE_ADD(call_date, INTERVAL IF(MINUTE(call_date) >= 58, 1, 0) HOUR),
                            '%H')                                                          hours,
                substring(dialog, 11, 4)                                                   set_queue,
                last_step,
                if(last_step not in ('', '0', '111', '371', '372', '362', '361', '261', '262') and
                   client_status in ('refusing', 'MeetingWait', 'Wait', 'CallWait'), 1, 0) talk,
                if(client_status = 'MeetingWait', 1, 0)                                    meeting
from suitecrm_robot.jc_robot_log
where last_step not in ('', '0', '111', '371', '372', '362', '361', '261', '262')
  and date(call_date) = date(now()) - interval 1 day
  and (deleted = 0 or deleted is null)
  and (inbound_call = 0 or inbound_call is null)