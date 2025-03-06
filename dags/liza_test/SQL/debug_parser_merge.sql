select uniqueid,
       date_entered,
       dialog,
       SUBSTRING(server_number, 7, 2) server_number,
       event_type,
       sec
  from suitecrm_robot.robot_debug_parser
 where date(date_entered) = '{}'
 
