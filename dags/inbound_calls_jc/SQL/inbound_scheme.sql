SELECT phone, 
       timestamp calldate_shhema, 
       date(timestamp)                                                                             call_date, 
       if(second(timestamp) > 50 and minute(timestamp) = 59, hour(timestamp) + 1, hour(timestamp)) call_hour, 
       case 
           when second(timestamp) > 50 and minute(timestamp) = 59 then 0 
           when second(timestamp) > 50 then minute(timestamp) + 1 
           else minute(timestamp) end                                                              call_minute, 
       queue destination_shhema, 
       exit_point exit_point_shhema , 1 as disposition_cdr 
FROM suitecrm.inbound_calls_info 
where date(timestamp) = date(now()) - interval 1 day