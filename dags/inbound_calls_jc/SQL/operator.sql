select asterisk_caller_id_c              phone, 
       date_entered                      calldate_zh, 
       date(date_entered)                call_date, 
       if(second(date_entered) > 54 and minute(date_entered) = 59, hour(date_entered) + 4, 
          hour(date_entered) + 3)        call_hour, 
       case 
           when second(date_entered) > 54 and minute(date_entered) = 59 then 0 
           when second(date_entered) > 54 then minute(date_entered) + 1 
           else minute(date_entered) end call_minute, 
       result_call_c                     client_status_zh, 
       assigned_user_id                  assigned_user_id_zh, 
       queue_c                           queue_zh , 1 as disposition_cdr, 0 as disposition_ro
from suitecrm.calls 
         left join suitecrm.calls_cstm on id = id_c 
where date(date_entered) = date(now()) - interval 1 day
  and direction = 'I' 
  and deleted = 0 