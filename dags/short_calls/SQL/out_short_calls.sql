with incoming_calls as (select cl.date_entered           as call_date,
                               cl_c.asterisk_caller_id_c as phone,
                               if((cl.assigned_user_id in ('', ' ') or cl.assigned_user_id is null), 'unknown_id',
                                  cl.assigned_user_id)   as user_call
                        from suitecrm.calls as cl
                                 left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                        where date(cl.date_entered) = date(now()) - interval 1 day
                          and direction='Inbound'
                          and duration_minutes < 10),


     outgoing_calls as (select cl.date_entered                  as call_date,
                               cl_c.asterisk_caller_id_c        as phone,
                               count(distinct cl_c.asterisk_caller_id_c) as phone_c,
                               if((cl.assigned_user_id in ('', ' ') or cl.assigned_user_id is null), 'unknown_id',
                                  cl.assigned_user_id)          as user_call
                        from suitecrm.calls as cl
                                 left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                        where date(cl.date_entered) = date(now()) - interval 1 day
                          and direction = 'Outbound'
         group by phone,date(call_date),user_call)


SELECT incoming_calls.phone        AS phone,
       DATE(incoming_calls.call_date) call_date,
       incoming_calls.user_call,
       if(phone_c is null, 0, phone_c) countout
FROM incoming_calls
         LEFT JOIN
     outgoing_calls
     ON incoming_calls.phone = outgoing_calls.phone and incoming_calls.user_call = outgoing_calls.user_call;



