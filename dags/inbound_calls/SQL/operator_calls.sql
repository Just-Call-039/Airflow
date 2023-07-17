select name,
       direction,
       assigned_user_id,
       status,
       if(asterisk_caller_id_c like '7%', concat('8',substring(asterisk_caller_id_c,2,10)), asterisk_caller_id_c) as asterisk_caller_id_c,
       result_call_c,
       otkaz_c,
       ne_reshena_c,
       reshena_c,
       date(date_start) calldate,
       queue_c
from suitecrm.calls
         left join suitecrm.calls_cstm on id = id_c
where date(date_start) between '2023-04-01' and date(now()) - interval 1 day
  -- and queue_c not in  (90003,9005,9293,9295,9296,9297,9298,9299)
  and queue_c != 90003
  and name = 'Входящий звонок'