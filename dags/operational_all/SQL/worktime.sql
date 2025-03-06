with timework as (select id_user, date, sum(A) timework, sum(talk_inbound) talk_inbound, sum(waiting) waiting
                  from (
                           SELECT `id_user`,
                                  `date`,
                                  (`recall` + `sobranie` + `obuchenie` + `training` + `nastavnik` + `problems` +
                                   `fact` + `pause10` - `progul_obrabotka_in_fact`) A,
                                  (fact - talk_inbound - talk_outbound - recall - recall_talk - obrabotka -
                                   progul_obrabotka_in_fact)                        waiting,
                                  talk_inbound
                           FROM suitecrm.reports_cache
                           where (date between '2022-12-01' and date(now()))
                             and id_user not in ('1', '')
                             and id_user is not null) t
                  group by id_user, date),
     config as (select D.name                                        'name',
                       step.name                                     'step_name',
                       index_number                                  'step',
                       replace(replace(queue, '_NEW^', ''), '^', '') 'dialogs',
                       custom_queue_c                                'custom'
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
                           end                                                    as queue_18
                from config
                where dialogs is not null
                  and dialogs != ''),
     steps as (select *
               from (
                        select step,
                               queue_1 as queue
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
                    ) as t2
               where queue is not null),
     ocheredi as (select queue,
                         date,
                         project_name project
                  from (select queue,
                               date,
                               project_type,
                               project,
                               row_number() over (partition by queue, date order by queue, date) rwn,
                               case
                                   when queue in (9203,9065,9205,9009,9201,9212,9219) then 'RTK LIDS'
                                   when queue = 9077 then 'MTS'
                                   when queue = 9128 then 'DOMRU Dop'
                                   when queue = 9153 then 'Almatel'
                                   when queue in (9020, 9133, 9024, 9047, 9041, 9043) then 'MGTS'
                                   when queue in (9074) then '2com'
                                   when queue = 9101 then 'MTS'
                                   when project = 11 and project_type = 1 then 'MTS LIDS'
                                   when project = 11 then 'MTS'
                                   when project = 10 and project_type = 1 then 'BEELINE LIDS'
                                   when project = 10 then 'BEELINE'
                                   when project = 19 and project_type = 1 then 'NBN LIDS'
                                   when project = 19 then 'NBN'
                                   when project = 3 and project_type = 1 then 'DOMRU LIDS'
                                   when project = 3 then 'DOMRU'
                                   when project = 5 and project_type = 1 then 'RTK LIDS'
                                   when project = 5 then 'RTK'
                                   when project = 6 and project_type = 1 then 'TTK LIDS'
                                   when project = 6 then 'TTK'
                                   when project in (12, 13) then 'BEELINE (sim)'
                                   else 'DR' end                                                 project_name
                        from suitecrm.queue_project
                        where date >= '2022-02-01') as t1
                  where rwn = 1),
     teams as (select *,
                      case
                           when left(first_name, instr(first_name, ' ') - 1) > 0 and
                                left(first_name, instr(first_name, ' ') - 1) < 10000
                               then left(first_name, instr(first_name, ' ') - 1)
                           when left(first_name, 2) = '�_'
                               then substring(first_name, 3, (instr(first_name, ' ') - 3))
                           when left(first_name, 1) = '�'
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
     jc as (select distinct assigned_user_id,
                            team,
                            jc.dialog,
                            if(destination_queue is null, jc.dialog, destination_queue) destination,
                            jc.uniqueid,
                            jc.phone,
                            date(jc.call_date)                                                            calldate,
                            jc.city_c
            from (select assigned_user_id,
                        REGEXP_SUBSTR(jc1.dialog, '[0-9]+')                                                   dialog,
                        jc1.uniqueid,
                        destination_queue,
                        jc1.phone,
                        call_date,
                        city_c,
                        team,
                        last_step
                   from suitecrm_robot.jc_robot_log jc1
                   left join steps on (queue = jc1.dialog and last_step = step)
                     left join (select distinct * from suitecrm.transferred_to_other_queue) tr
                               on tr.phone = jc1.phone and tr.uniqueid = jc1.uniqueid
                     left join teams on assigned_user_id = teams.id
            where assigned_user_id not in ('', '1')
              and date(call_date) between '2024-09-01' and date(now())
              and step is not null

                    union all

                 select operator_id assigned_user_id,
                        robot_id dialog,
                        dialog_id as uniqueid,
                        transfer as destination_queue,
                        jc2.phone,
                        call_date,
                        city city_c,
                        team,
                        last_step
                   from suitecrm_robot.robot_log jc2
                        left join suitecrm_robot.robot_log_addition jc3
                             on jc2.id = jc3.robot_log_id
                
                     left join steps on (queue = robot_id and last_step = step)
                     left join teams on jc3.operator_id = teams.id
            where jc3.operator_id not in ('', '1')
              and date(call_date) between '2024-09-01' and date(now())
              and step is not null)  jc),
     jc2 as (select jc.*,
                    case
                       
                       when team = 555 then 'RTK'
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
                       when team in (14, 33, 34, 36, 39, 41, 44, 61, 118, 121, 123, 141) then 'TTK LIDS'

                       when destination in
                            (9001, 9003, 9036, 9038, 9042, 9049, 9051, 9068, 9072, 9017, 9081, 9262, 9082, 9084, 9094,
                             9099, 9111, 9224,9296,9272,9269,9261) then 'BEELINE'
                       when destination in
                            (9006, 9019, 9024, 9031, 9047, 9052, 9053, 9058, 9071, 9080, 9083, 9096, 9119, 9128, 9225,
                             9232, 9237, 9239, 9242, 9257, 9292, 9211) then 'DOMRU LIDS'
                       when destination in
                            (9028, 9012, 9164, 9045, 9029, 9016, 9023, 9041, 9075, 9077, 9078, 9085, 9131, 9133, 9170,
                             9191, 9227, 9264, 9270, 9270, 9180, 9022,9180,9033,9041,9273,9045) then 'MTS'
                       when destination in
                            (9010, 9179, 9043, 9178, 9044, 0, 9048, 9073, 9184, 9106, 9120, 9121, 9137, 9141, 9267,
                             9142, 9263, 9143, 9021, 9145, 9173, 9177, 9208, 9169, 9210, 9185, 9176, 9211, 9220, 9138,
                             9221, 9162, 9174, 9222, 9223, 9717, 9226, 9229, 9228, 9196, 9230, 9140, 9241, 9251,
                             9252, 9263, 9267, 9268) then 'MTS LIDS'
                       when destination in (9040, 9055, 9118, 9244, 9063, 9298) then 'NBN'
                       when destination in
                            (9007, 9013, 9291, 9025, 9291, 9027, 9050, 9036, 9255, 9030, 9060, 9213, 9061, 9062, 9076,
                             9289, 9037, 9289, 9087, 9038, 9076, 9090, 9059, 9091, 9098, 9101, 9102, 9103, 9105, 9115,
                             9158, 9175, 9184, 9290, 9200, 9202, 9206, 9213, 9204, 9214, 9034, 9245, 9234, 9215, 9218,
                             9233, 9243, 9247, 9248, 9249, 9253, 9254, 9256, 9089, 9240, 9048) then 'RTK LIDS'
                       when destination in
                            (9011, 9014, 9039, 9056, 9070, 9079, 9092, 9095, 9100, 9110, 9117, 9116, 9026, 9132, 9217,
                             9261, 9269, 9274, 9275, 9276, 9277, 9278, 9280, 9288) then 'TTK LIDS'
                       when destination = 9057 then 'NBN LIDS'
                       when destination in (9271,9099) then 'DR'

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
                       project
             from jc
                      left join ocheredi on (destination = queue and date = calldate)),
     jc3 as (select assigned_user_id,
                    team,
                    dialog,
                    destination,
                    uniqueid,
                    phone,
                    calldate,
                    case
                        when team in (28, 50, 12) and
                             project in ('RTK LIDS', 'TTK LIDS', 'MTS LIDS', 'NBN LIDS', 'BEELINE LIDS', 'DOMRU LIDS')
                            then REPLACE(project, ' LIDS', '')
                        else project end finalproject
             from jc2),
     jc4 as (select assigned_user_id, dialog, destination, calldate, finalproject, team, count(uniqueid) calls
             from jc3
             group by 1, 2, 3, 4, 5, 6)

select jc4.*,
       sum(calls) over (partition by assigned_user_id,calldate) all_calls,
       timework,
       waiting,
       talk_inbound
from jc4
         left join timework on (id_user = assigned_user_id and date = calldate);