with calls as (select cl.id,
                      date(cl.date_entered)     as call_date,
                      cl.date_entered           as full_call_date,
                      cl.name,
                      cl_c.asterisk_caller_id_c as phone,
                      if((cl_c.queue_c in ('', ' ') or cl_c.queue_c is null), 'unknown_queue',
                         cl_c.queue_c)          as queue,
                      if((cl.assigned_user_id in ('', ' ') or cl.assigned_user_id is null), 'unknown_id',
                         cl.assigned_user_id)   as user_call,
                      if((cl_c.user_id_c in ('', ' ') or cl_c.user_id_c is null), 'unknown_id',
                         cl_c.user_id_c)        as super,
                      cl_c.result_call_c,
                      cl_c.otkaz_c,
                      case
                          when city_c is null then concat(town_c, 'OBL')
                          when city_c in ('', ' ') then concat(town_c, 'OBL')
                          else city_c
                          end                   as city
               from suitecrm.calls as cl
                        left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                        left join suitecrm.contacts on cl_c.asterisk_caller_id_c = contacts.phone_work
                        left join suitecrm.contacts_cstm on contacts_cstm.id_c = contacts.id
               where day(date(cl.date_entered)) != day(curdate())
                 and month(date(cl.date_entered)) = month(curdate())
                 and year(date(cl.date_entered)) = year(curdate())),

     ws as (select *
            from (select *, row_number() over (partition by id_user order by date_start desc) as num
                  from suitecrm.worktime_supervisor) as temp
            where temp.num = 1)

select calls.id,
       calls.call_date,
       calls.full_call_date,
       calls.name,
       calls.phone,
       calls.queue,
       calls.user_call,
       if((ws.supervisor in ('', ' ') or ws.supervisor is null), 'unknown_id', ws.supervisor) as super,
       calls.result_call_c,
       calls.otkaz_c,
       calls.city
from calls
         left join ws on calls.user_call = ws.id_user;
