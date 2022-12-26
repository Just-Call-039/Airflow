select case
           when cl_1.assigned_user_id is null or cl_1.assigned_user_id = '' or cl_1.assigned_user_id = ' '
               then 'unknown_id'
           else cl_1.assigned_user_id end as user,
       case
           when cl.queue_c is null or cl.queue_c = '' or cl.queue_c = ' '
               then 'unknown_queue'
           else cl.queue_c end            as queue,
       cl.otkaz_c                         as ref,
       date(cl_1.date_entered)            as calls_date,
       case
           when cl.user_id_c is null or cl.user_id_c = '' or cl.user_id_c = ' '
               then 'unknown_id'
           else cl.user_id_c end          as super,
       case
           when city_c is null then concat(town_c, 'OBL')
           when city_c in ('', ' ') then concat(town_c, 'OBL')
           else city_c
           end                            as city
from suitecrm.calls_cstm as cl
         left join suitecrm.calls as cl_1 on cl.id_c = cl_1.id
         left join suitecrm.contacts on cl.asterisk_caller_id_c = contacts.phone_work
         left join suitecrm.contacts_cstm on contacts_cstm.id_c = contacts.id
where cl.result_call_c = 'refusing'
  and cl_1.date_entered >= '2022-01-01';
