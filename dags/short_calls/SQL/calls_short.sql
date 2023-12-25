select distinct cl.id,
       date(cl.date_entered)               as call_date,
       DATE_FORMAT(DATE_ADD(cl.date_entered, INTERVAL IF(MINUTE(cl.date_entered) >= 58, 1, 0) HOUR),
                   '%H')                      hours,
       cl.name,
       cl_c.asterisk_caller_id_c           as phone,
       if((cl_c.queue_c in ('', ' ') or cl_c.queue_c is null), 'unknown_queue',
          cl_c.queue_c)                    as queue,
       if((cl.assigned_user_id in ('', ' ') or cl.assigned_user_id is null), 'unknown_id',
          cl.assigned_user_id)             as user_call,
       duration_minutes                    as call_sec,
       if(cl.duration_minutes <= 10, 1, 0) as short_calls,
       completed_c,
       result_call_c,
        otkaz_c
from suitecrm.calls as cl
         left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
where date(cl.date_entered) = date(now()) - interval 1 day and direction='Inbound'




