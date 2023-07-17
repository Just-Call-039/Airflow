select TT.*,
       id contact_id
from (select if(name = 'Входящий звонок', 1, 0) name,
             direction,
             assigned_user_id,
             calls.status,
             if(asterisk_caller_id_c like '7%', concat('8', substring(asterisk_caller_id_c, 2, 10)),
                asterisk_caller_id_c) as        asterisk_caller_id_c,
             result_call_c,
             otkaz_c,
             ne_reshena_c,
             reshena_c,
             date(date_start)                   calldate
      from suitecrm.calls
               left join suitecrm.calls_cstm on id = id_c
               left join users on assigned_user_id = users.id
      where date(date_start) between '2023-06-01' and date(now()) - interval 1 day
        and queue_c = 90003
        and duration_minutes > 0
     ) TT
         left join suitecrm.contacts
                   on asterisk_caller_id_c = phone_work