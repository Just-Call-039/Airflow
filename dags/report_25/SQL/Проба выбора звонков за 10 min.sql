select call_date + interval 2 hour            as my_date,
       now(),
       timediff(time(now()), time(call_date)) as r,
       uniqueid,
       substring(dialog, 11, 4)               as ochered,
       last_step,
       billsec,
       phone
from suitecrm_robot.jc_robot_log
# where (date(call_date) = date(now()))
where (date(call_date) = '2022-04-21')
  and (timediff(time(now()), time(call_date)) between '03:15:00' and '03:24:59')
order by my_date desc;
