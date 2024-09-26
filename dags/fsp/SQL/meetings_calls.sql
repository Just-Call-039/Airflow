with config as (select index_number                                  step,
                       replace(replace(queue, '_NEW^', ''), '^', '') dialogs,
                       case when action_type>0 and check_exit = 1 then '0'
                        when action_type>0 and check_exit = 0 then '1' end type_steps
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
                        select step,
                               queue_1 as ochered,
                               type_steps
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
     config2 as (select substring(turn, 11, 4) ochered, steps_transferred step, '1' type_steps
                 from suitecrm.jc_robot_reportconfig
                 where deleted = 0
                   and substring(turn, 11, 4) not in (select ochered from steps)
                   and steps_transferred != ''),
     tabeue2 as (select step,
                        ochered,
                        type_steps,
                        SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 1), ',', -1) as step_1,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 2), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 1), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 2), ',', -1)
                            end                                                 as step_2,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 3), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 2), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 3), ',', -1)
                            end                                                 as step_3,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 4), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 3), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 4), ',', -1)
                            end                                                 as step_4,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 5), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 4), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 5), ',', -1)
                            end                                                 as step_5,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 6), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 5), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 6), ',', -1)
                            end                                                 as step_6,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 7), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 6), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 7), ',', -1)
                            end                                                 as step_7,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 8), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 7), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 8), ',', -1)
                            end                                                 as step_8,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 9), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 8), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 9), ',', -1)
                            end                                                 as step_9,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 10), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 9), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 10), ',', -1)
                            end                                                 as step_10,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 11), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 10), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 11), ',', -1)
                            end                                                 as step_11,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 12), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 11), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 12), ',', -1)
                            end                                                 as step_12,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 13), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 12), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 13), ',', -1)
                            end                                                 as step_13,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 14), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 13), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 14), ',', -1)
                            end                                                 as step_14,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 15), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 14), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 15), ',', -1)
                            end                                                 as step_15,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 16), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 15), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 16), ',', -1)
                            end                                                 as step_16,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 17), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 16), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 17), ',', -1)
                            end                                                 as step_17,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 18), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 17), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 18), ',', -1)
                            end                                                 as step_18,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 19), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 18), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 19), ',', -1)
                            end                                                 as step_19,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 20), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 19), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 20), ',', -1)
                            end                                                 as step_20,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 21), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 20), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 21), ',', -1)
                            end                                                 as step_21,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 22), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 21), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 22), ',', -1)
                            end                                                 as step_22,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 23), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 22), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 23), ',', -1)
                            end                                                 as step_23,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 24), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 23), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 24), ',', -1)
                            end                                                 as step_24,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 25), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 24), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 25), ',', -1)
                            end                                                 as step_25,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 26), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 25), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 26), ',', -1)
                            end                                                 as step_26,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 27), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 26), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 27), ',', -1)
                            end                                                 as step_27,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 28), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 27), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 28), ',', -1)
                            end                                                 as step_28,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 29), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 28), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 29), ',', -1)
                            end                                                 as step_29,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 30), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 29), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 30), ',', -1)
                            end                                                 as step_30,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 31), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 20), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 31), ',', -1)
                            end                                                 as step_31,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 32), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 31), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 32), ',', -1)
                            end                                                 as step_32,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 33), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 32), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 33), ',', -1)
                            end                                                 as step_33,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 34), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 33), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 34), ',', -1)
                            end                                                 as step_34,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 35), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 34), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 35), ',', -1)
                            end                                                 as step_35,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 36), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 35), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 36), ',', -1)
                            end                                                 as step_36,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 37), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 36), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 37), ',', -1)
                            end                                                 as step_37,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 38), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 37), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 38), ',', -1)
                            end                                                 as step_38,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 39), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 38), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 39), ',', -1)
                            end                                                 as step_39,
                        case
                            when
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 40), ',', -1) =
                                    SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 39), ',', -1)
                                then null
                            else SUBSTRING_INDEX(SUBSTRING_INDEX(step, ',', 40), ',', -1)
                            end                                                 as step_40
                 from config2),
     steps2 as (select *
               from (
                        select step,
                               queue_1 as ochered, type_steps
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
                        union all
                        select step_1, ochered, type_steps
                        from tabeue2
                        union all
                        select step_2
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_3
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_4
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_5
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_6
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_7
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_8
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_9
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_10
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_11
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_12
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_13
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_14
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_15
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_16
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_17
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_18
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_19
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_20
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_21
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_22
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_23
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_24
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_25
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_26
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_27
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_28
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_29
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_30
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_31
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_32
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_33
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_34
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_35
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_36
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_37
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_38
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_39
                             , ochered, type_steps
                        from tabeue2
                        union all
                        select step_40
                             , ochered, type_steps
                        from tabeue2
                    ) as t2
               where ochered is not null and step is not null)




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
       if(jc2.city_c is null or jc2.city_c in ('',0),concat(jc2.town,'_t'),jc2.city_c) as city_c,
              case
           when (ptv_c like '%^3^%'
               or ptv_c like '%^5^%'
               or ptv_c like '%^6^%'
               or ptv_c like '%^10^%'
               or ptv_c like '%^11^%'
               or ptv_c like '%^19^%'
               or ptv_c like '%^14^%') then 'ptv_1'
           when (base_source_c like '%220%' or base_source_c like '%221%' or base_source_c like '%222%' or
                 base_source_c like '%223%' or base_source_c like '%224%') then 'bno'

           when base_source_c like '%63%' and region_c in (1, 3) then '63_1'
           when base_source_c like '%63%' and region_c in (2, 4, 5, 6, 7) then concat('63_', region_c)
           when base_source_c like '%63%' and (region_c is null or region_c in ('', ' ')) then '63_0'

           when base_source_c like '%62%' and region_c in (1, 3) then '62_1'
           when base_source_c like '%62%' and region_c in (2, 4, 5, 6, 7) then concat('62_', region_c)
           when base_source_c like '%62%' and (region_c is null or region_c in ('', ' ')) then '62_0'

           when base_source_c like '%60%' and region_c in (1, 3) then '60_1'
           when base_source_c like '%60%' and region_c in (2, 4, 5, 6, 7) then concat('60_', region_c)
           when base_source_c like '%60%' and (region_c is null or region_c in ('', ' ')) then '60_0'

           when base_source_c like '%61%' and region_c in (1, 3) then '61_1'
           when base_source_c like '%61%' and region_c in (2, 4, 5, 6, 7) then concat('61_', region_c)
           when base_source_c like '%61%' and (region_c is null or region_c in ('', ' ')) then '61_0'

           else region_c end region_c2,
#        city_c,
                   directory,
                   trunk_id
#        if(datecall < '2023-04-01',city_c,city) as city_c
from (select distinct jc.uniqueid                    id,
                      DATE(jc.call_date)             datecall,
                      jc.assigned_user_id,
                      jc.phone,
                      jc.city_c,
                      jc.town,
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
                      base_source_c,
                   directory,
                   trunk_id
      from (select uniqueid,
                   call_date,
                   assigned_user_id,
                   phone,
                   city_c,
                   town,
                   network_provider_c,
                   ptv_c,
                   region_c,
                   marker,
                   last_step,
                   dialog,
                   was_repeat,
                   inbound_call,
                   directory,
                   trunk_id
            from suitecrm_robot.jc_robot_log
            WHERE date(call_date) >= '2023-07-01'
              and jc_robot_log.assigned_user_id not in ('1', '')
            union all
            select uniqueid,
                   call_date,
                   assigned_user_id,
                   phone,
                   city_c,
                   town,
                   network_provider_c,
                   ptv_c,
                   region_c,
                   marker,
                   last_step,
                   dialog,
                   was_repeat,
                   inbound_call,
                   directory,
                   trunk_id
            from suitecrm_robot.jc_robot_log_2024_05 jc1
            WHERE jc1.assigned_user_id not in ('1', '')
          union all
            select uniqueid,
                   call_date,
                   assigned_user_id,
                   phone,
                   city_c,
                   town,
                   network_provider_c,
                   ptv_c,
                   region_c,
                   marker,
                   last_step,
                   dialog,
                   was_repeat,
                   inbound_call ,
                   directory,
                   trunk_id
            from suitecrm_robot.jc_robot_log_2024_04 jc2
            WHERE jc2.assigned_user_id not in ('1', '')
union all
            select uniqueid,
                   call_date,
                   assigned_user_id,
                   phone,
                   city_c,
                   town,
                   network_provider_c,
                   ptv_c,
                   region_c,
                   marker,
                   last_step,
                   dialog,
                   was_repeat,
                   inbound_call ,
                   directory,
                   trunk_id
            from suitecrm_robot.jc_robot_log_2024_07 jc3
            WHERE jc3.assigned_user_id not in ('1', '')
          union all
            select uniqueid,
                   call_date,
                   assigned_user_id,
                   phone,
                   city_c,
                   town,
                   network_provider_c,
                   ptv_c,
                   region_c,
                   marker,
                   last_step,
                   dialog,
                   was_repeat,
                   inbound_call ,
                   directory,
                   trunk_id
            from suitecrm_robot.jc_robot_log_2024_06 jc3
            WHERE jc3.assigned_user_id not in ('1', '')
           ) jc
               left join suitecrm.contacts c on c.phone_work = jc.phone
               left join suitecrm.contacts_cstm cm on cm.id_c = c.id
               left join (select distinct * from suitecrm.transferred_to_other_queue) tr
                         on jc.uniqueid = tr.uniqueid and tr.phone = jc.phone
      WHERE
#             date(call_date) >= '2023-10-01'
#         and
            jc.assigned_user_id not in ('1', '')) jc2


where last_step in (select distinct step
from steps2)
# or inbound_call = 1

