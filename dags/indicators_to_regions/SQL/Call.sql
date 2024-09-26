
-- select date(calls.date_entered)                                                                              datecall,
--        DATE_FORMAT(DATE_ADD(calls.date_entered, INTERVAL IF(MINUTE(calls.date_entered) >= 58, 1, 0) HOUR), '%H') + 3 hoursonly,
--        assigned_user_id                                                                              userid,
--        queue_c,
--        result_call_c,
--        otkaz_c,
--        project_c,
--        if(length(replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ',
--                                            '')) <=
--                             10,
--                             concat(8,
--                                    replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ', '')),
--                             concat(8,
--                                    right(replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ',
--                                                  ''), 10))) as phone,
--        duration_minutes
-- from suitecrm.calls
--          left join suitecrm.calls_cstm on id = id_c
--          left join suitecrm.users on assigned_user_id = users.id
-- where DATE_FORMAT(calls.date_entered, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
--                     AND calls.date_entered < DATE_SUB(CURDATE(), INTERVAL 0 DAY);

select date(calls.date_entered)                                                                              datecall,
       DATE_FORMAT(DATE_ADD(calls.date_entered, INTERVAL IF(MINUTE(calls.date_entered) >= 58, 1, 0) HOUR), '%H') + 3 hoursonly,
       calls.assigned_user_id                                                                              userid,
       
       contacts_cstm.city_c,
       queue_c,
       result_call_c,
       calls_cstm.otkaz_c,
       project_c,
       if(length(replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as phone,
       duration_minutes
from suitecrm.calls
         left join suitecrm.calls_cstm on id = id_c
         left join suitecrm.users on assigned_user_id = users.id
         left join suitecrm.contacts on asterisk_caller_id_c = contacts.phone_work
         left join suitecrm.contacts_cstm on contacts.id = contacts_cstm.id_c
         
where DATE_FORMAT(calls.date_entered, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
                    AND calls.date_entered < DATE_SUB(CURDATE(), INTERVAL 0 DAY);