with config as (select D.name                                        'Название диалога',
                       step.name,
#                            'Название шага перевода',
                       index_number                                  'step',
                       replace(replace(queue, '_NEW^', ''), '^', '') dialogs,
                       custom_queue_c                                'Принимающая очередь',
                       step.description
                from suitecrm.jc_robconfig_step step
                         left join suitecrm.jc_robconfig_step_cstm cstm on step.id = cstm.id_c
                         left join suitecrm.jc_robconfig_dialog_jc_robconfig_step_c sv
                                   on step.id = sv.jc_robconfig_dialog_jc_robconfig_stepjc_robconfig_step_idb
                         left join suitecrm.jc_robconfig_dialog D
                                   on D.id = sv.jc_robconfig_dialog_jc_robconfig_stepjc_robconfig_dialog_ida
                where action_type > 0),

     tabeue as (select step,
                       dialogs,
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
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 232), ',', -1)
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
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 30), ',', -1)
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
                           end                                                    as queue_40,
                       config.name,
                       config.description
                from config
                where dialogs is not null
                  and dialogs != ''),

     steps as (select *
               from (
                        select step,
                               queue_1 as ochered
                        from tabeue
                        union all
                        select step, queue_2
                        from tabeue
                        union all
                        select step, queue_3
                        from tabeue
                        union all
                        select step, queue_4
                        from tabeue
                        union all
                        select step, queue_5
                        from tabeue
                        union all
                        select step, queue_6
                        from tabeue
                        union all
                        select step, queue_7
                        from tabeue
                        union all
                        select step, queue_8
                        from tabeue
                        union all
                        select step, queue_9
                        from tabeue
                        union all
                        select step, queue_10
                        from tabeue
                        union all
                        select step, queue_11
                        from tabeue
                        union all
                        select step, queue_12
                        from tabeue
                        union all
                        select step, queue_13
                        from tabeue
                        union all
                        select step, queue_14
                        from tabeue
                        union all
                        select step, queue_15
                        from tabeue
                        union all
                        select step, queue_16
                        from tabeue
                        union all
                        select step, queue_17
                        from tabeue
                        union all
                        select step, queue_18
                        from tabeue
                        union all
                        select step, queue_19
                        from tabeue
                        union all
                        select step, queue_20
                        from tabeue
                        union all
                        select step, queue_21
                        from tabeue
                        union all
                        select step, queue_22
                        from tabeue
                        union all
                        select step, queue_23
                        from tabeue
                        union all
                        select step, queue_24
                        from tabeue
                        union all
                        select step, queue_25
                        from tabeue
                        union all
                        select step, queue_26
                        from tabeue
                        union all
                        select step, queue_27
                        from tabeue
                        union all
                        select step, queue_28
                        from tabeue
                        union all
                        select step, queue_29
                        from tabeue
                        union all
                        select step, queue_30
                        from tabeue
                        union all
                        select step, queue_31
                        from tabeue
                        union all
                        select step, queue_32
                        from tabeue
                        union all
                        select step, queue_33
                        from tabeue
                        union all
                        select step, queue_34
                        from tabeue
                        union all
                        select step, queue_35
                        from tabeue
                        union all
                        select step, queue_36
                        from tabeue
                        union all
                        select step, queue_37
                        from tabeue
                        union all
                        select step, queue_38
                        from tabeue
                        union all
                        select step, queue_39
                        from tabeue
                        union all
                        select step, queue_40
                        from tabeue
                    ) as t2
               where ochered is not null)

select jc2.id,
       datecall,
       jc2.assigned_user_id,
       phone,
       network_provider,
       jc2.ptv_c,
       jc2.region_c,
       marker,
       last_step,
       queue,
       destination_queue,
       was_repeat,
       inbound_call,
       city_c,
                   directory
#        if(datecall < '2023-04-01',city_c,city) as city_c
from (select distinct jc.uniqueid                    id,
                      DATE(jc.call_date)             datecall,
                      jc.assigned_user_id,
                      jc.phone,
                      jc.city_c,
                      if(jc.city_c is null or jc.city_c = '',concat(cm.town_c,'_t'),jc.city_c) as city,
                      case
                          when jc.network_provider_c = '83' then 'МТС'
                          when jc.network_provider_c = '80' then 'Билайн'
                          when jc.network_provider_c = '82' then 'Мегафон'
                          when jc.network_provider_c = '10' then 'Теле2'
                          when jc.network_provider_c = '68' then 'Теле2'
                          else 'MVNO'
                          end                        network_provider,
                      jc.ptv_c,
                      jc.region_c,
                      jc.marker,
                      jc.last_step,
                      substring(jc.dialog, 11, 4) as queue,
                      destination_queue,
                      was_repeat,
                      inbound_call,
                   directory
      from (select uniqueid,
                   call_date,
                   assigned_user_id,
                   phone,
                   city_c,
                   network_provider_c,
                   ptv_c,
                   region_c,
                   marker,
                   last_step,
                   dialog,
                   was_repeat,
                   inbound_call,
                   directory
            from suitecrm_robot.jc_robot_log
            WHERE date(call_date) >= '2022-09-01'
              and jc_robot_log.assigned_user_id not in ('1', '')
            union all
            select uniqueid,
                   call_date,
                   assigned_user_id,
                   phone,
                   city_c,
                   network_provider_c,
                   ptv_c,
                   region_c,
                   marker,
                   last_step,
                   dialog,
                   was_repeat,
                   inbound_call,
                   directory
            from suitecrm_robot.jc_robot_log_2023_07 jc1
            WHERE jc1.assigned_user_id not in ('1', '')
          union all
            select uniqueid,
                   call_date,
                   assigned_user_id,
                   phone,
                   city_c,
                   network_provider_c,
                   ptv_c,
                   region_c,
                   marker,
                   last_step,
                   dialog,
                   was_repeat,
                   inbound_call ,
                   directory
            from suitecrm_robot.jc_robot_log_2023_06 jc2
            WHERE jc2.assigned_user_id not in ('1', '')
union all
            select uniqueid,
                   call_date,
                   assigned_user_id,
                   phone,
                   city_c,
                   network_provider_c,
                   ptv_c,
                   region_c,
                   marker,
                   last_step,
                   dialog,
                   was_repeat,
                   inbound_call ,
                   directory
            from suitecrm_robot.jc_robot_log_2023_05 jc3
            WHERE jc3.assigned_user_id not in ('1', '')
           ) jc
               left join suitecrm.contacts c on c.phone_work = jc.phone
               left join suitecrm.contacts_cstm cm on cm.id_c = c.id
               left join (select distinct * from suitecrm.transferred_to_other_queue) tr
                         on jc.uniqueid = tr.uniqueid and tr.phone = jc.phone
      WHERE date(call_date) >= '2022-10-01'
        and jc.assigned_user_id not in ('1', '')) jc2
         left join steps on last_step = step and queue = ochered
where step is not null
# or inbound_call = 1