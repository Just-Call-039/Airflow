select date(call_date)          calldate,
       phone,
       substring(dialog, 11, 4) dialog,
       jc_robot_log.assigned_user_id,
       last_step,
       client_status,
       real_billsec,
       contacts_cstm.city_c,
       town_c
from suitecrm_robot.jc_robot_log
  left join suitecrm.contacts
                   on phone = phone_work
         left join suitecrm.contacts_cstm on contacts.id = id_c
where DATE_FORMAT(call_date, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
  AND call_date < DATE_SUB(CURDATE(), INTERVAL 0 DAY)
  and last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
  and substring(dialog, 11, 4) in ('9293', '9033', '9296','9297','9298', '9299')