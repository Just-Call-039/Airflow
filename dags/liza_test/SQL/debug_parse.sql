select uniqueid,
       date(date_entered) call_date,
       dialog,
       SUBSTRING(server_number, 7, 2) server_number,
       event_type,
       round(AVG(sec), 2) parse_sec
  from suitecrm_robot.robot_debug_parser
 where date(date_entered) = '{}'
 group by uniqueid, event_type
