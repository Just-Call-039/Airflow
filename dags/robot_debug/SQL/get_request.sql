select * 
  from suitecrm_robot.robot_debug_parser
where date(date_entered) = date(now()) - interval 1 day