with cl as (select calls.assigned_user_id,
                   calls.status,
                   if(length(replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ',
                                     '')) <=
                      10,
                      concat(8,
                             replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ',
                                     '')),
                      concat(8,
                             right(replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''),
                                           ' ',
                                           ''), 10))) as asterisk_caller_id_c,
                   result_call_c,
                   otkaz_c,
                   date(calls.date_entered)              calldate
            from suitecrm.calls
                     left join suitecrm.calls_cstm on id = id_c
            where (otkaz_c = 'otkaz_23' or otkaz_c = 'otkaz_42' or otkaz_c = 'no_answer')
              and date(calls.date_entered) >= '2023-11-01'
              and (result_call_c is null
                or result_call_c = 'refusing')),

     plan as (select distinct count(phone)        phone,
                              dateentered         call_date,
                              tt.assigned_user_id user_call,
                              'planned'           types,
                              town_c,
                              city_c,
                              dialog
              from (
                       select jc_planned_calls.assigned_user_id,
                              date(jc_planned_calls.date_entered)                                                                 dateentered,
                              jc_planned_calls.phone,
                              row_number() over (partition by jc_planned_calls.phone order by jc_planned_calls.date_entered desc) row,
                              contacts_cstm.city_c,
                              contacts_cstm.town_c,
                              substring(dialog, 11, 4)                                                                            dialog
                       from jc_planned_calls
                                left join users on users.id = assigned_user_id
                                join jc_planned_calls_cstm on jc_planned_calls.id = id_c
                                left join suitecrm.contacts
                                          on phone = contacts.phone_work
                                left join suitecrm.contacts_cstm on contacts.id = contacts_cstm.id_c
                                left join suitecrm_robot.jc_robot_log
                                          on jc_planned_calls.phone = jc_robot_log.phone
                       where DATE_FORMAT(jc_planned_calls.date_entered, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
  AND jc_planned_calls.date_entered < DATE_SUB(CURDATE(), INTERVAL 0 DAY)
                         and contacts_status is null
                         and date_start < now()) tt
              where row = 1
              group by user_call, call_date, city_c, town_c
              union
              select count(phone)        phone,
                     dateentered         call_date,
                     tt.assigned_user_id user_call,
                     'planned'           types,
                     town_c,
                     city_c,
                     dialog
              from (
                       select jc_planned_calls.assigned_user_id,
                              date(jc_planned_calls.date_entered)                                                                 dateentered,
                              jc_planned_calls.phone,
                              row_number() over (partition by jc_planned_calls.phone order by jc_planned_calls.date_entered desc) row,
                              contacts_status,
                              contacts_cstm.city_c,
                              contacts_cstm.town_c,
                              substring(dialog, 11, 4)                                                                            dialog
                       from jc_planned_calls
                                left join users on users.id = assigned_user_id
                                join jc_planned_calls_cstm on jc_planned_calls.id = id_c
                                left join suitecrm.contacts
                                          on phone = contacts.phone_work
                                left join suitecrm.contacts_cstm on contacts.id = contacts_cstm.id_c
                                left join suitecrm_robot.jc_robot_log
                                          on jc_planned_calls.phone = jc_robot_log.phone
                       where DATE_FORMAT(jc_planned_calls.date_entered, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
  AND jc_planned_calls.date_entered < DATE_SUB(CURDATE(), INTERVAL 0 DAY)
                         and date_start < now()) tt
                       join cl on asterisk_caller_id_c = phone
              where row = 1
              group by user_call, call_date, city_c, town_c),

     incoming_calls as (select cl.date_entered           as call_date,
                               cl_c.asterisk_caller_id_c as phone,
                               if((cl.assigned_user_id in ('', ' ') or cl.assigned_user_id is null), 'unknown_id',
                                  cl.assigned_user_id)   as user_call,
                               substring(dialog, 11, 4)     dialog
                        from suitecrm.calls as cl
                                 left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                                 left join suitecrm_robot.jc_robot_log
                                           on phone = asterisk_caller_id_c and
                                              date(call_date) = date(cl.date_entered)
                        where DATE_FORMAT(cl.date_entered, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
  AND cl.date_entered < DATE_SUB(CURDATE(), INTERVAL 0 DAY)
                          and direction = 'Inbound'
                          and duration_minutes < 10),


     outgoing_calls as (select cl.date_entered                           as call_date,
                               cl_c.asterisk_caller_id_c                 as phone,
                               count(distinct cl_c.asterisk_caller_id_c) as phone_c,
                               if((cl.assigned_user_id in ('', ' ') or cl.assigned_user_id is null), 'unknown_id',
                                  cl.assigned_user_id)                   as user_call
                        from suitecrm.calls as cl
                                 left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                        where DATE_FORMAT(cl.date_entered, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
  AND cl.date_entered < DATE_SUB(CURDATE(), INTERVAL 0 DAY)
                          and direction = 'Outbound'
                          and duration_minutes < 10
                        group by phone, date(call_date), user_call),
     obryv as (SELECT count(incoming_calls.phone) AS phone,
                      DATE(incoming_calls.call_date) call_date,
                      incoming_calls.user_call       user_call,
                      'outcall'                      types,
                      town_c,
                      city_c,
                      dialog
               FROM incoming_calls
                        left join outgoing_calls
                                  ON incoming_calls.phone = outgoing_calls.phone and
                                     incoming_calls.user_call = outgoing_calls.user_call and
                                     incoming_calls.call_date = outgoing_calls.call_date
                        left join suitecrm.contacts
                                  on incoming_calls.phone = phone_work
                        left join suitecrm.contacts_cstm on id = id_c
               group by call_date, user_call, types)


select *
from obryv
union all
select *
from plan



