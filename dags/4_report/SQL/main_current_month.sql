select cl.id,
       date(cl.date_entered)                                                                     as call_date,
       cl.name,
       cl_c.asterisk_caller_id_c                                                                 as phone,
#        cl_c.project_c,
       if((cl_c.queue_c in ('', ' ') or cl_c.queue_c is null), 'unknown_queue',
          cl_c.queue_c)                                                                          as queue,
       if((cl.assigned_user_id in ('', ' ') or cl.assigned_user_id is null), 'unknown_id',
          cl.assigned_user_id)                                                                   as user_call,
       if((cl_c.user_id_c in ('', ' ') or cl_c.user_id_c is null), 'unknown_id', cl_c.user_id_c) as super,
       case
           when city_c is null then concat(town_c, 'OBL')
           when city_c in ('', ' ') then concat(town_c, 'OBL')
           else city_c
           end                                                                                   as city,
       duration_minutes                                                                          as call_sec,
#        cl.duration_minutes div 60                                                                as call_min,
#        cl.duration_minutes % 60                                                                  as call_sec,
       if(cl.duration_minutes <= 10, 1, 0)                                                       as short_calls
from suitecrm.calls as cl
         left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
         left join suitecrm.contacts on cl_c.asterisk_caller_id_c = contacts.phone_work
         left join suitecrm.contacts_cstm on contacts_cstm.id_c = contacts.id
where day(date(cl.date_entered)) != day(curdate())
  and month(date(cl.date_entered)) = month(curdate())
  and year(date(cl.date_entered)) = year(curdate());
