with config as (select D.name                                                   '???????? ???????',
                       step.name                                                '???????? ???? ????????',
                       index_number                                             'step',
                       replace(replace(queue, '_NEW^', ''), '^', '')            'dialogs',
                       custom_queue_c                                           '??????????? ???????',
                       case
                           when action_type > 0 and check_exit = 1 then '0'
                           when action_type > 0 and check_exit = 0 then '1' end type_steps
                from suitecrm.jc_robconfig_step step
                         left join suitecrm.jc_robconfig_step_cstm cstm on step.id = cstm.id_c
                         left join suitecrm.jc_robconfig_dialog_jc_robconfig_step_c sv
                                   on step.id = sv.jc_robconfig_dialog_jc_robconfig_stepjc_robconfig_step_idb
                         left join suitecrm.jc_robconfig_dialog D
                                   on D.id = sv.jc_robconfig_dialog_jc_robconfig_stepjc_robconfig_dialog_ida
                where action_type > 0),
     tabeue as (select step,
                       dialogs,
                       type_steps,
                       SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 1), ',', -1) as queue_1,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 2), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 1), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 2), ',', -1)
                           end                                                    as queue_2,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 3), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 2), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 3), ',', -1)
                           end                                                    as queue_3,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 4), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 3), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 4), ',', -1)
                           end                                                    as queue_4,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 5), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 4), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 5), ',', -1)
                           end                                                    as queue_5,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 6), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 5), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 6), ',', -1)
                           end                                                    as queue_6,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 7), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 6), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 7), ',', -1)
                           end                                                    as queue_7,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 8), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 7), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 8), ',', -1)
                           end                                                    as queue_8,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 9), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 8), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 9), ',', -1)
                           end                                                    as queue_9,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 10), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 9), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 10), ',', -1)
                           end                                                    as queue_10,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 11), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 10), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 11), ',', -1)
                           end                                                    as queue_11,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 12), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 11), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 12), ',', -1)
                           end                                                    as queue_12,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 13), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 12), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 13), ',', -1)
                           end                                                    as queue_13,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 14), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 13), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 14), ',', -1)
                           end                                                    as queue_14,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 15), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 14), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 15), ',', -1)
                           end                                                    as queue_15,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 16), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 15), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 16), ',', -1)
                           end                                                    as queue_16,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 17), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 16), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 17), ',', -1)
                           end                                                    as queue_17,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 18), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 17), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 18), ',', -1)
                           end                                                    as queue_18,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 19), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 18), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 19), ',', -1)
                           end                                                    as queue_19,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 20), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 19), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 20), ',', -1)
                           end                                                    as queue_20,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 21), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 20), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 21), ',', -1)
                           end                                                    as queue_21,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 22), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 21), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 22), ',', -1)
                           end                                                    as queue_22,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 23), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 22), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 23), ',', -1)
                           end                                                    as queue_23,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 24), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 23), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 24), ',', -1)
                           end                                                    as queue_24,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 25), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 24), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 25), ',', -1)
                           end                                                    as queue_25,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 26), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 25), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 26), ',', -1)
                           end                                                    as queue_26,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 27), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 26), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 27), ',', -1)
                           end                                                    as queue_27,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 28), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 27), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 28), ',', -1)
                           end                                                    as queue_28,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 29), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 28), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 29), ',', -1)
                           end                                                    as queue_29,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 30), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 29), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 30), ',', -1)
                           end                                                    as queue_30,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 31), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 20), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 31), ',', -1)
                           end                                                    as queue_31,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 32), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 31), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 32), ',', -1)
                           end                                                    as queue_32,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 33), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 32), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 33), ',', -1)
                           end                                                    as queue_33,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 34), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 33), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 34), ',', -1)
                           end                                                    as queue_34,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 35), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 34), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 35), ',', -1)
                           end                                                    as queue_35,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 36), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 35), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 36), ',', -1)
                           end                                                    as queue_36,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 37), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 36), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 37), ',', -1)
                           end                                                    as queue_37,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 38), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 37), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 38), ',', -1)
                           end                                                    as queue_38,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 39), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 38), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 39), ',', -1)
                           end                                                    as queue_39,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 40), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 39), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 40), ',', -1)
                           end                                                    as queue_40
                from config
                where dialogs is not null
                  and dialogs != ''),
     steps as (select *
               from (
                        select step, queue_1 as ochered, type_steps
                        from tabeue
                        union all
                        select step, queue_2, type_steps
                        from tabeue
                        union all
                        select step, queue_3, type_steps
                        from tabeue
                        union all
                        select step, queue_4, type_steps
                        from tabeue
                        union all
                        select step, queue_5, type_steps
                        from tabeue
                        union all
                        select step, queue_6, type_steps
                        from tabeue
                        union all
                        select step, queue_7, type_steps
                        from tabeue
                        union all
                        select step, queue_8, type_steps
                        from tabeue
                        union all
                        select step, queue_9, type_steps
                        from tabeue
                        union all
                        select step, queue_10, type_steps
                        from tabeue
                        union all
                        select step, queue_11, type_steps
                        from tabeue
                        union all
                        select step, queue_12, type_steps
                        from tabeue
                        union all
                        select step, queue_13, type_steps
                        from tabeue
                        union all
                        select step, queue_14, type_steps
                        from tabeue
                        union all
                        select step, queue_15, type_steps
                        from tabeue
                        union all
                        select step, queue_16, type_steps
                        from tabeue
                        union all
                        select step, queue_17, type_steps
                        from tabeue
                        union all
                        select step, queue_18, type_steps
                        from tabeue
                        union all
                        select step, queue_19, type_steps
                        from tabeue
                        union all
                        select step, queue_20, type_steps
                        from tabeue
                        union all
                        select step, queue_21, type_steps
                        from tabeue
                        union all
                        select step, queue_22, type_steps
                        from tabeue
                        union all
                        select step, queue_23, type_steps
                        from tabeue
                        union all
                        select step, queue_24, type_steps
                        from tabeue
                        union all
                        select step, queue_25, type_steps
                        from tabeue
                        union all
                        select step, queue_26, type_steps
                        from tabeue
                        union all
                        select step, queue_27, type_steps
                        from tabeue
                        union all
                        select step, queue_28, type_steps
                        from tabeue
                        union all
                        select step, queue_29, type_steps
                        from tabeue
                        union all
                        select step, queue_30, type_steps
                        from tabeue
                        union all
                        select step, queue_31, type_steps
                        from tabeue
                        union all
                        select step, queue_32, type_steps
                        from tabeue
                        union all
                        select step, queue_33, type_steps
                        from tabeue
                        union all
                        select step, queue_34, type_steps
                        from tabeue
                        union all
                        select step, queue_35, type_steps
                        from tabeue
                        union all
                        select step, queue_36, type_steps
                        from tabeue
                        union all
                        select step, queue_37, type_steps
                        from tabeue
                        union all
                        select step, queue_38, type_steps
                        from tabeue
                        union all
                        select step, queue_39, type_steps
                        from tabeue
                        union all
                        select step, queue_40, type_steps
                        from tabeue
                    ) as t2
               where ochered is not null),

     stepsss as (select step, ochered, type_steps
                 from steps),
     cl as (select calls.assigned_user_id,
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
            where (otkaz_c = 'otkaz_23' or otkaz_c = 'otkaz_42' or otkaz_c = 'no_answer' or otkaz_c = 'no_ansver' or
                   otkaz_c = '' or duration_minutes <= 10)
              and date(calls.date_entered) = date(now())
              and (result_call_c is null
                or result_call_c = 'refusing')),

     plan as (select distinct phone,
                              dateentered calldate,
                              'Перезвон'  type,
                              town_c,
                              city_c,
                              dialog,
                              ''          last_step
              from (
                       select jc_planned_calls.assigned_user_id,
                              date(jc_planned_calls.date_entered)                                                                 dateentered,
                              jc_planned_calls.phone,
                              row_number() over (partition by jc_planned_calls.phone order by jc_planned_calls.date_entered desc) row,
                              contacts_cstm.city_c,
                              contacts_cstm.town_c,
                              substring(dialog, 11, 4)                                                                            dialog,
                              contacts.assigned_user_id                                                                           user_call
                       from jc_planned_calls
                                left join users on users.id = assigned_user_id
                                join jc_planned_calls_cstm on jc_planned_calls.id = id_c
                                left join suitecrm.contacts
                                          on phone = contacts.phone_work
                                left join suitecrm.contacts_cstm on contacts.id = contacts_cstm.id_c
                                left join suitecrm_robot.jc_robot_log
                                          on jc_planned_calls.phone = jc_robot_log.phone
                       where date(jc_planned_calls.date_entered)
                           BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 MONTH) AND CURDATE()
                         and (contacts_status is null or (contacts_status = 'CallWait' and
                                                          (jc_robot_log.otkaz = 'otkaz_23' or
                                                           jc_robot_log.otkaz = 'otkaz_42' or
                                                           jc_robot_log.otkaz = 'no_answer' or
                                                           jc_robot_log.otkaz = 'no_ansver' or
                                                           jc_robot_log.otkaz = '')))
                         and date(date_start) = date(now())) tt
              where row = 1
              union
              select phone,
                     dateentered calldate,
                     'Перезвон'  type,
                     town_c,
                     city_c,
                     dialog,
                     ''          last_step
              from (
                       select jc_planned_calls.assigned_user_id,
                              date(jc_planned_calls.date_entered)                                                                 dateentered,
                              jc_planned_calls.phone,
                              row_number() over (partition by jc_planned_calls.phone order by jc_planned_calls.date_entered desc) row,

                              contacts_status,
                              contacts_cstm.city_c,
                              contacts_cstm.town_c,
                              substring(dialog, 11, 4)                                                                            dialog,
                              jc_planned_calls.assigned_user_id                                                                   user_call
                       from jc_planned_calls
                                left join users on users.id = assigned_user_id
                                join jc_planned_calls_cstm on jc_planned_calls.id = id_c
                                left join suitecrm.contacts
                                          on phone = contacts.phone_work
                                left join suitecrm.contacts_cstm on contacts.id = contacts_cstm.id_c
                                left join suitecrm_robot.jc_robot_log
                                          on jc_planned_calls.phone = jc_robot_log.phone
                       where date(jc_planned_calls.date_entered)
                           BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 MONTH) AND CURDATE()
                         and date(date_start) = date(now())) tt
                       join cl on asterisk_caller_id_c = phone
              where row = 1),

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
                        where date(cl.date_entered) = date(now())
                          and direction = 'Inbound'
                          and duration_minutes < 10),


     outgoing_calls as (select cl.date_entered                           as call_date,
                               cl_c.asterisk_caller_id_c                 as phone,
                               count(distinct cl_c.asterisk_caller_id_c) as phone_c,
                               if((cl.assigned_user_id in ('', ' ') or cl.assigned_user_id is null), 'unknown_id',
                                  cl.assigned_user_id)                   as user_call
                        from suitecrm.calls as cl
                                 left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                        where date(cl.date_entered) = date(now())
                          and direction = 'Outbound'
                          and duration_minutes < 10
                        group by phone, date(call_date), user_call),
     obryv as (SELECT incoming_calls.phone,
                      DATE(incoming_calls.call_date) calldate,
                      'Обрыв'                        type,
                      town_c,
                      city_c,
                      dialog,
                      ''                             last_step
               FROM incoming_calls
                        left join outgoing_calls
                                  ON incoming_calls.phone = outgoing_calls.phone and
                                     incoming_calls.user_call = outgoing_calls.user_call and
                                     incoming_calls.call_date = outgoing_calls.call_date
                        left join suitecrm.contacts
                                  on incoming_calls.phone = phone_work
                        left join suitecrm.contacts_cstm on id = id_c
     ),

     poteri as (select phone,
                       date(call_date)          calldate,
                       'Потеряшка'              type,
                       town_c,
                       contacts_cstm.city_c,
                       substring(dialog, 11, 4) dialog,
                       last_step
                from suitecrm_robot.jc_robot_log
                         left join suitecrm.contacts
                                   on phone = phone_work
                         left join suitecrm.contacts_cstm on contacts.id = id_c
                         join stepsss on last_step = step and ochered = substring(dialog, 11, 4)
                where jc_robot_log.assigned_user_id = 1
                  and client_status = 'null_status'
                  and date(call_date) = date(now())
                  and last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372'))


select phone,
       calldate,
       dialog,
       tt.town_c       town_c,
       last_step,
       case
           when contacts_cstm.ptv_c like '%^3^%'
               or contacts_cstm.ptv_c like '%^5^%'
               or contacts_cstm.ptv_c like '%^6^%'
               or contacts_cstm.ptv_c like '%^10^%'
               or contacts_cstm.ptv_c like '%^11^%'
               or contacts_cstm.ptv_c like '%^19^%'
               or contacts_cstm.ptv_c like '%^14^%' then 'Разметка Наша'
           when contacts_cstm.ptv_c like '%^3_19^%'
               or contacts_cstm.ptv_c like '%^5_19^%'
               or contacts_cstm.ptv_c like '%^6_19^%'
               or contacts_cstm.ptv_c like '%^10_19^%'
               or contacts_cstm.ptv_c like '%^11_19^%'
               or contacts_cstm.ptv_c like '%^19_19^%'
               or contacts_cstm.ptv_c like '%^14_19^%' then 'Разметка не наша 50+'
           when
                       contacts_cstm.ptv_c like '%^3_21^%'
                   or contacts_cstm.ptv_c like '%^5_21^%'
                   or contacts_cstm.ptv_c like '%^6_21^%'
                   or contacts_cstm.ptv_c like '%^10_21^%'
                   or contacts_cstm.ptv_c like '%^11_21^%'
                   or contacts_cstm.ptv_c like '%^19_21^%'
                   or contacts_cstm.ptv_c like '%^14_21^%' then 'Разметка не наша Телеком'
           when
                       contacts_cstm.ptv_c like '%^3_18^%'
                   or contacts_cstm.ptv_c like '%^5_18^%'
                   or contacts_cstm.ptv_c like '%^6_18^%'
                   or contacts_cstm.ptv_c like '%^10_18^%'
                   or contacts_cstm.ptv_c like '%^11_18^%'
                   or contacts_cstm.ptv_c like '%^19_18^%'
                   or contacts_cstm.ptv_c like '%^14_18^%' then 'Разметка не наша 50-40'
           when
                       contacts_cstm.ptv_c like '%^5_20^%'
                   or contacts_cstm.ptv_c like '%^3_20^%'
                   or contacts_cstm.ptv_c like '%^6_20^%'
                   or contacts_cstm.ptv_c like '%^10_20^%'
                   or contacts_cstm.ptv_c like '%^11_20^%'
                   or contacts_cstm.ptv_c like '%^19_20^%'
                   or contacts_cstm.ptv_c like '%^14_20^%' then 'Разметка не наша Спутник'
           when
                       contacts_cstm.ptv_c like '%^3_17^%'
                   or contacts_cstm.ptv_c like '%^5_17^%'
                   or contacts_cstm.ptv_c like '%^6_17^%'
                   or contacts_cstm.ptv_c like '%^10_17^%'
                   or contacts_cstm.ptv_c like '%^11_17^%'
                   or contacts_cstm.ptv_c like '%^19_17^%'
                   or contacts_cstm.ptv_c like '%^14_17^%' then 'Разметка не наша 40-30'
           when
                       contacts_cstm.ptv_c like '%^5_16^%'
                   or contacts_cstm.ptv_c like '%^3_16^%'
                   or contacts_cstm.ptv_c like '%^6_16^%'
                   or contacts_cstm.ptv_c like '%^10_16^%'
                   or contacts_cstm.ptv_c like '%^11_16^%'
                   or contacts_cstm.ptv_c like '%^19_16^%'
                   or contacts_cstm.ptv_c like '%^14_16^%' then 'Разметка не наша 30-20'
           when
                       contacts_cstm.ptv_c like '%^5_15^%'
                   or contacts_cstm.ptv_c like '%^3_15^%'
                   or contacts_cstm.ptv_c like '%^6_15^%'
                   or contacts_cstm.ptv_c like '%^10_15^%'
                   or contacts_cstm.ptv_c like '%^11_15^%'
                   or contacts_cstm.ptv_c like '%^19_15^%'
                   or contacts_cstm.ptv_c like '%^14_15^%' then 'Разметка не наша 20-0'
           when contacts_cstm.region_c = 1 then 'Наша полная'
           when contacts_cstm.region_c = 2 then 'Наша неполная'
           when contacts_cstm.region_c = 4 then 'Фиас из разных источников'
           when contacts_cstm.region_c = 5 then 'Фиас до города'
           when contacts_cstm.region_c = 6 then 'Старый town_c'
           when contacts_cstm.region_c = 7 then 'Def code'
           else '' end ptv,
       type
from (
         select *
         from obryv
         union all
         select *
         from plan
         union all
         select *
         from poteri) tt
         left join suitecrm.contacts
                   on tt.phone = phone_work
         left join suitecrm.contacts_cstm on id = id_c


