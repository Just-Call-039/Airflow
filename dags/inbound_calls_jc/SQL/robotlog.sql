SELECT distinct jc_robot_log.phone, 
                call_date                             calldate_ro, 
                date(call_date)                       call_date, 
                if(second(call_date) > 54 and minute(call_date) = 59, hour(call_date) + 4, 
                   hour(call_date) + 3)               call_hour, 
                case 
                    when second(call_date) > 54 and minute(call_date) = 59 then 0 
                    when second(call_date) > 54 then minute(call_date) + 1 
                    else minute(call_date) end        call_minute, 
                substring(jc_robot_log.dialog, 11, 4) queue_ro, 
                destination_queue destination_queue_ro, 
                jc_robot_log.uniqueid, 
                billsec billsec_ro, 
                jc_robot_log.assigned_user_id assigned_user_id_ro, 
                substring(route, 1, 3)                first_step_ro, 
                last_step last_step_ro, 
                client_status client_status_ro, 
                server_number server_number_ro , 1 as disposition_cdr , 1 as disposition_ro
FROM suitecrm_robot.jc_robot_log 
         left join suitecrm.users u on jc_robot_log.assigned_user_id = u.id 
         left join (select distinct * from suitecrm.transferred_to_other_queue) tr 
                   on jc_robot_log.uniqueid = tr.uniqueid and tr.phone = jc_robot_log.phone 
where date(call_date) = date(now()) - interval 1 day
  and (route like '%262%' 
    or route like '%362%' 
    or route like '%372%' 
    ) 
  and jc_robot_log.deleted = 0 