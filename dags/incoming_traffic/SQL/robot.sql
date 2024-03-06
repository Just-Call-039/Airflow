select phone,
       date(call_date)          calldate,
       substring(dialog, 11, 4) dialog,
       contacts_cstm.city_c,
       town_c,
       last_step
from suitecrm_robot.jc_robot_log
         left join suitecrm.contacts
                   on phone = contacts.phone_work
         left join suitecrm.contacts_cstm on contacts.id = contacts_cstm.id_c
where DATE_FORMAT(call_date, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
  AND call_date < DATE_SUB(CURDATE(), INTERVAL 0 DAY)
  and last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')