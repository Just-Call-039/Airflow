select name,
       direction,
       assigned_user_id,
       status,
       if(length(replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ',
                                           '')) <=
                            10,
                            concat(8,
                                   replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ', '')),
                            concat(8,
                                   right(replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ',
                                                 ''), 10))) as asterisk_caller_id_c,
       result_call_c,
       otkaz_c,
       ne_reshena_c,
       reshena_c,
       date(date_start)                     calldate,
       queue_c
from suitecrm.calls
         left join suitecrm.calls_cstm on id = id_c
where date(date_start) between '2023-05-01' and date(now()) - interval 1 day
  and queue_c != 90003
  and name = 'Входящий звонок'