with ocheredi as (select *
                  from (select queue,
                               project,
                               date,
                               (row_number() over (partition by queue order by date desc)) as rw
                        from suitecrm.queue_project
                        where date >= '2022-02-01') as tb1
                  where rw = 1
                  order by 3),
     department as (select '12' team, 'Входящая линия' department
                    union all
                    select '50' team, 'Диспетчера Алексеевой' department
                    union all
                    select '28' team, 'Универсалы' department
                    union all
                    select '4' team, 'Диспетчера Кротченко' department
                    union all
                    select '42' team, 'Авито' department
                    union all
                    select '16' team, 'Банкроты' department),
     config as (select D.name                                        'Название диалога',
                       step.name                                     'Название шага перевода',
                       index_number                                  'step',
                       replace(replace(queue, '_NEW^', ''), '^', '') 'dialogs',
                       custom_queue_c                                'Принимающая очередь'
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
               where ochered is not null),
     teams as (select *,
                      case
                          when left(first_name, instr(first_name, ' ') - 1) > 0 and
                               left(first_name, instr(first_name, ' ') - 1) < 10000
                              then left(first_name, instr(first_name, ' ') - 1)
                          when left(first_name, 2) = 'я_'
                              then substring(first_name, 3, (instr(first_name, ' ') - 3))
                          when left(first_name, 1) = 'я'
                              then substring(first_name, 2, (instr(first_name, ' ') - 1))
                          when first_name > 0 then first_name
                          else '' end team
               from (
                        SELECT id,
                               concat(first_name, ' ', last_name) fio,
                               case
                                   when substring_index(substring_index(first_name, ' ', 3), ' ', -1) REGEXP '^[0-9]+$'
                                       then substring_index(substring_index(first_name, ' ', 3), ' ', -1)
                                   when substring_index(substring_index(first_name, ' ', 4), ' ', -1) REGEXP '^[0-9]+$'
                                       then substring_index(substring_index(first_name, ' ', 4), ' ', -1)
                                   else first_name
                                   end                            first_name
                        FROM suitecrm.users) user),
     Q as (SELECT distinct jc_robot_log.assigned_user_id,
                           base_source_c,
                           date(call_date) as                    datec,
                           substring(jc_robot_log.dialog, 11, 4) queue,
                           substring(jc_robot_log.dialog, 11, 4) Last_queue,
                           IF(destination_queue is null, substring(jc_robot_log.dialog, 11, 4),
                              destination_queue)                 destination_queue,
                           case
                               when jc_robot_log.network_provider_c = '83' then 'МТС'
                               when jc_robot_log.network_provider_c = '80' then 'Билайн'
                               when jc_robot_log.network_provider_c = '82' then 'Мегафон'
                               when jc_robot_log.network_provider_c = '10' then 'Теле2'
                               when jc_robot_log.network_provider_c = '68' then 'Теле2'
                               else 'MVNO'
                               end                               network_provider_c,
                           if(jc_robot_log.city_c is null or jc_robot_log.city_c = '',concat(cm.town_c,'_t'),jc_robot_log.city_c) as city_c,
                           jc_robot_log.region_c,
                           jc_robot_log.ptv_c,
                           jc_robot_log.uniqueid                 id,
                           jc_robot_log.phone,
                           marker,
                           last_step
           FROM suitecrm_robot.jc_robot_log
                    left join suitecrm.contacts c on c.phone_work = jc_robot_log.phone
                    left join suitecrm.contacts_cstm cm on cm.id_c = c.id
                    left join teams on jc_robot_log.assigned_user_id = teams.id
                    left join steps on (substring(jc_robot_log.dialog, 11, 4) = ochered and step = last_step)
                    left join (select distinct * from suitecrm.transferred_to_other_queue) tr
                              on jc_robot_log.uniqueid = tr.uniqueid and tr.phone = jc_robot_log.phone
           WHERE client_status IS NOT NULL
             and jc_robot_log.deleted = 0
             AND jc_robot_log.assigned_user_id != '1'
             and step is not null
             and (inbound_call = 0 or inbound_call = '')
             and date(call_date) = date(now()) - interval {} day
     ),
     Q1 as (select assigned_user_id,
                   base_source_c,
                   datec,
                   Q.queue,
                   destination_queue,
                   network_provider_c,
                   city_c,
                   region_c,
                   ptv_c,
                   Q.id,
                   team,
                   marker,
                   last_step,
                   case
                       when team = 555 and datec <= '2023-02-15' then 'NBN'
                       when team = 555 and datec > '2023-02-15' then 'RTK'
		               when team = 123 then 'RTK'
                       when team in (122, 432, 667) then 'BEELINE LIDS'
                       when team in (20, 24, 40, 62, 63, 90, 100, 502, 503, 504, 506, 507, 509, 510, 511, 512, 513)
                           then 'DOMRU LIDS'
                       when team in (15, 25, 27, 30, 432) then 'MTS'
                       when team in
                            (6, 7, 18, 23, 55, 56, 57, 58, 64, 65, 73, 74, 75, 76, 77, 81, 87, 91, 101, 102, 103, 104, 117,
                             202) then 'MTS LIDS'
                       when team in (8, 32) then 'NBN'
                       when team in (45, 46, 49, 53) then 'NBN LIDS'
                       when team in
                            (1, 2, 3, 5, 11, 11, 11, 16, 17, 21, 26, 26, 29, 31, 35, 37, 38, 43, 47, 48, 51, 54, 59, 60,
                             66, 67, 68, 69, 71, 78, 79, 83, 84, 85, 86, 89, 92, 93, 94, 95, 96, 97, 98, 99, 105, 106,
                             109, 113, 115, 116, 120, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 135, 136, 137,
                             138, 139, 140, 666, 689) then 'RTK LIDS'
                       when team in (14, 33, 34, 36, 39, 41, 44, 61, 118, 121, 141) then 'TTK LIDS'

                       when destination_queue in
                            (9001, 9003, 9036, 9038, 9042, 9049, 9051, 9068, 9072, 9017, 9081, 9262, 9082, 9084, 9094,
                             9099, 9111, 9224,9296,9272,9269,9261) then 'BEELINE'
                       when destination_queue in
                            (9006, 9019, 9024, 9031, 9047, 9052, 9053, 9058, 9071, 9080, 9083, 9096, 9119, 9128, 9225,
                             9232, 9237, 9239, 9242, 9257, 9292, 9211) then 'DOMRU LIDS'
                       when destination_queue in
                            (9028, 9012, 9164, 9045, 9029, 9016, 9023, 9041, 9075, 9077, 9078, 9085, 9131, 9133, 9170,
                             9191, 9227, 9264, 9270, 9270, 9180, 9022,9180,9033,9041,9273,9045) then 'MTS'
                       when destination_queue in
                            (9010, 9179, 9043, 9178, 9044, 9073, 9184, 9106, 9120, 9121, 9137, 9141, 9267,
                             9142, 9263, 9143, 9021, 9145, 9173, 9177, 9208, 9169, 9210, 9185, 9176, 9211, 9220, 9138,
                             9221, 9162, 9174, 9222, 9223, 9717, 9226, 9229, 9228, 9196, 9230, 9140, 9241, 9251,
                             9252, 9263, 9267, 9268) then 'MTS LIDS'
                       when destination_queue in (9040, 9055, 9118, 9244, 9063, 9298) then 'NBN'
                       when destination_queue in
                            (9007, 9013, 9291, 9025, 9291, 9027, 9050, 9036, 9255, 9030, 9060, 9213, 9061, 9062, 9076,
                             9289, 9037, 9289, 9087, 9038, 9076, 9090, 9059, 9091, 9098, 9101, 9102, 9103, 9105, 9115,
                             9158, 9175, 9184, 9290, 9200, 9202, 9206, 9213, 9204, 9214, 9034, 9245, 9234, 9215, 9218,
                             9233, 9243, 9247, 9248, 9249, 9253, 9254, 9256, 9089, 9240, 9048) then 'RTK LIDS'
                       when destination_queue in
                            (9011, 9014, 9039, 9056, 9070, 9079, 9092, 9095, 9100, 9110, 9117, 9116, 9026, 9132, 9217,
                             9261, 9269, 9274, 9275, 9276, 9277, 9278, 9280, 9288) then 'TTK LIDS'
                       when destination_queue = 9057 then 'NBN LIDS'
                       when destination_queue in (9271,9099) then 'DR'

                       when project = 10 then 'BEELINE'
                       when project = 11 then 'MTS'
                       when project = 19 then 'NBN'
                       when project = 3 then 'DOMRU LIDS'
                       when project = 6 then 'TTK LIDS'
                       when project = 5 then 'RTK LIDS'
                       when project = 12 then 'BEELINE (sim)'
                       when project = 13 then 'MTS (sim)'
                       else 'DR'
                       end
                       project1
            from Q
                     left join ocheredi on destination_queue = ocheredi.queue
                     left join teams on Q.assigned_user_id = teams.id),
     Q2 as (select *,
                  case
                      when team in (12, 13, 50, 4) and
                           project1 in ('RTK', 'TTK', 'MTS', 'NBN', 'BEELINE', 'DOMRU')
                          then concat(project1, ' LIDS')
                      else project1 end proect,
                  ''                    data_type,
                  case
                      when (ptv_c like '%^3^%'
                          or ptv_c like '%^5^%'
                          or ptv_c like '%^6^%'
                          or ptv_c like '%^10^%'
                          or ptv_c like '%^11^%'
                          or ptv_c like '%^19^%'
                          or ptv_c like '%^14^%') then 'ptv_1'
                      when
                          (ptv_c like '%^3_19^%'
                              or ptv_c like '%^5_19^%'
                              or ptv_c like '%^6_19^%'
                              or ptv_c like '%^10_19^%'
                              or ptv_c like '%^11_19^%'
                              or ptv_c like '%^19_19^%'
                              or ptv_c like '%^14_19^%'
                              or ptv_c like '%^3_21^%'
                              or ptv_c like '%^5_21^%'
                              or ptv_c like '%^6_21^%'
                              or ptv_c like '%^10_21^%'
                              or ptv_c like '%^11_21^%'
                              or ptv_c like '%^19_21^%'
                              or ptv_c like '%^14_21^%'
                              or ptv_c like '%^3_18^%'
                              or ptv_c like '%^5_18^%'
                              or ptv_c like '%^6_18^%'
                              or ptv_c like '%^10_18^%'
                              or ptv_c like '%^11_18^%'
                              or ptv_c like '%^19_18^%'
                              or ptv_c like '%^14_18^%'
                              or ptv_c like '%^5_20^%'
                              or ptv_c like '%^3_20^%'
                              or ptv_c like '%^6_20^%'
                              or ptv_c like '%^10_20^%'
                              or ptv_c like '%^11_20^%'
                              or ptv_c like '%^19_20^%'
                              or ptv_c like '%^14_20^%'
                              or ptv_c like '%^3_17^%'
                              or ptv_c like '%^5_17^%'
                              or ptv_c like '%^6_17^%'
                              or ptv_c like '%^10_17^%'
                              or ptv_c like '%^11_17^%'
                              or ptv_c like '%^19_17^%'
                              or ptv_c like '%^14_17^%'
                              or ptv_c like '%^5_16^%'
                              or ptv_c like '%^3_16^%'
                              or ptv_c like '%^6_16^%'
                              or ptv_c like '%^10_16^%'
                              or ptv_c like '%^11_16^%'
                              or ptv_c like '%^19_16^%'
                              or ptv_c like '%^14_16^%'
                              or ptv_c like '%^5_15^%'
                              or ptv_c like '%^3_15^%'
                              or ptv_c like '%^6_15^%'
                              or ptv_c like '%^10_15^%'
                              or ptv_c like '%^11_15^%'
                              or ptv_c like '%^19_15^%'
                              or ptv_c like '%^14_15^%') then 'ptv_2'
                      else region_c end region
            from Q1),
     Q3 as (select Q2.*,
                   case
                       when department is null and locate('LIDS', proect) > 0 then 'Лиды'
                       when department is null then 'КЦ'
                       else department end department
            from Q2
                     left join department on Q2.team = department.team)

    select assigned_user_id,
    team,
                   datec,
                   queue,
                   destination_queue,
                   proect,
                   network_provider_c,
                   city_c,
                   region,
                   data_type,
                   department,
                   id,
                   marker,
                   last_step
            from Q3