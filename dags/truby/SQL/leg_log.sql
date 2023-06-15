select date(calldate) as calldate,
    hour(calldate) hour,
    pattern, gw_number, (number+10000000000) number
                      from suitecrm_robot.leg_log
                      where date(calldate) = date(now()) - interval {} day