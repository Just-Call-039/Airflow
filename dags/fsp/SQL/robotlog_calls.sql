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
 # clear_users as (select id,
 #                            concat(first_name, ' ', last_name) fio,
 #                            first_name,
 #                            case
 #                                when substring_index(substring_index(first_name, ' ', 3), ' ', -1) REGEXP '^[0-9]+$'
 #                                    then substring_index(substring_index(first_name, ' ', 3), ' ', -1)
 #                                when substring_index(substring_index(first_name, ' ', 4), ' ', -1) REGEXP '^[0-9]+$'
 #                                    then substring_index(substring_index(first_name, ' ', 4), ' ', -1)
 #                                else
 #                                    (case
 #                                         when left(first_name, instr(first_name, ' ') - 1) > 0 and
 #                                              left(first_name, instr(first_name, ' ') - 1) < 10000
 #                                             then left(first_name, instr(first_name, ' ') - 1)
 #                                         when left(first_name, 2) = 'я_'
 #                                             then substring(first_name, 3, (instr(first_name, ' ') - 3))
 #                                         when left(first_name, 1) = 'я'
 #                                             then substring(first_name, 2, (instr(first_name, ' ') - 1))
 #                                         else '' end)
 #                                end                            teams
 #                     from suitecrm.users),
 #     supervisors as (select id as super, fio as super_fio, replace(teams, ' ', '') team
 #                     from clear_users
 #                     where id in (select distinct supervisor
 #                                  from suitecrm.worktime_supervisor)),
 #     teams as (select clear_users.id, fio, date(date_start) start, if(date_stop is null, date(now()), date(date_stop)-interval 1 day) stop, super as supervisor, supervisors.team
 #         from clear_users
 #         left join suitecrm.worktime_supervisor on clear_users.id = id_user
 #         left join supervisors on super = supervisor),
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
 config as (select index_number                                  step,
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
               where ochered is not null and step is not null),
 R as (SELECT distinct date(call_date)                       call_date,
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
                           end                               network_provider,
                       if(jc_robot_log.city_c is null or jc_robot_log.city_c in ('',0),concat(cm.town_c,'_t'),jc_robot_log.city_c) as city_c,
                       # jc_robot_log.city_c,
                       jc_robot_log.region_c                 region,
                       trunk_id,
                       jc_robot_log.ptv_c,
                       cm.base_source_c,
                       jc_robot_log.uniqueid                 id,
                       billsec,
                       jc_robot_log.assigned_user_id,
                       real_billsec,
                       jc_robot_log.uniqueid,
                       jc_robot_log.phone,
                       if(type_steps = 1, 1, 0)   perevod,
                       if(type_steps = 0, 1, 0)   lead,
                       marker,
                       last_step,
                       if(SUBSTRING_INDEX(route, ',', 1) in (262, 362, 372), 1, 0) as inbound_call,
                  #     if(inbound_call = 1, 1, 0) as inbound_call,
                   directory
       FROM suitecrm_robot.jc_robot_log
                left join suitecrm.contacts c on c.phone_work = jc_robot_log.phone
                left join suitecrm.contacts_cstm cm on cm.id_c = c.id
                left join suitecrm.users u on jc_robot_log.assigned_user_id = u.id
                left join steps2 on (ochered = substring(dialog, 11, 4) and last_step = step)
                left join (select distinct * from suitecrm.transferred_to_other_queue) tr
                          on jc_robot_log.uniqueid = tr.uniqueid and tr.phone = jc_robot_log.phone
        WHERE date(call_date) = date(now()) - interval {} day
         # and (inbound_call = 0 or inbound_call = '')
         and jc_robot_log.deleted = 0),
 R2 as (SELECT call_date,
               R.queue,
               destination_queue,
               assigned_user_id,
               network_provider,
               city_c,
               region,
               trunk_id,
               ptv_c,
               base_source_c,
               R.id,
               if( last_step in ('','0','1','261','262','111','361','362','371','372') and billsec <= 2,0,billsec) as billsec,
               if( last_step in ('','0','1','261','262','111','361','362','371','372') and real_billsec <= 2,0,real_billsec) as real_billsec,
               perevod,
               lead,
               team,
               marker,
               last_step,
          #     type_steps,
               inbound_call,
                   directory,
               case
                   when team = 555 and call_date <= '2023-02-15' then 'NBN'
                   when team = 555 and call_date > '2023-02-15' then 'RTK'
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
                        (9010, 9179, 9043, 9178, 9044, 0, 9048, 9073, 9184, 9106, 9120, 9121, 9137, 9141, 9267,
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
                   project
        from R
#          left join ocheredi on (destination_queue = ocheredi.queue and call_date = ocheredi.date)
                 left join ocheredi on (destination_queue = ocheredi.queue)
                 left join teams on R.assigned_user_id = teams.id
                 # where (call_date between start and stop) or start is null
                 ),
 R3 as (select R2.*,
               case
                   when department is null and locate('LIDS', project) > 0 then 'Лиды'
                   when department is null then 'КЦ'
                   else department end department
        from R2
                 left join department on R2.team = department.team)
                 
select call_date,
               queue,
               destination_queue,
               assigned_user_id,
               network_provider,
               city_c,
               trunk_id,
               perevod,
               lead,
               id,
               billsec,
               real_billsec,
               department,
               marker,
               team,
               directory,
               last_step,
          #     type_steps,
               inbound_call,
 case
                      when team in (12, 13, 50, 4) and
                           project in ('RTK', 'TTK', 'MTS', 'NBN', 'BEELINE', 'DOMRU')
                          then concat(project, ' LIDS')
                   else project end proect,
               ''                   data_type,
case when (ptv_c like '%^3^%'
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
                           else region end region_c,
# region as region_c,
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

           when base_source_c like '%63%' and region in (1, 3) then '63_1'
           when base_source_c like '%63%' and region in (2, 4, 5, 6, 7) then concat('63_', region)
           when base_source_c like '%63%' and (region is null or region in ('', ' ')) then '63_0'

           when base_source_c like '%62%' and region in (1, 3) then '62_1'
           when base_source_c like '%62%' and region in (2, 4, 5, 6, 7) then concat('62_', region)
           when base_source_c like '%62%' and (region is null or region in ('', ' ')) then '62_0'

           when base_source_c like '%60%' and region in (1, 3) then '60_1'
           when base_source_c like '%60%' and region in (2, 4, 5, 6, 7) then concat('60_', region)
           when base_source_c like '%60%' and (region is null or region in ('', ' ')) then '60_0'

           when base_source_c like '%61%' and region in (1, 3) then '61_1'
           when base_source_c like '%61%' and region in (2, 4, 5, 6, 7) then concat('61_', region)
           when base_source_c like '%61%' and (region is null or region in ('', ' ')) then '61_0'

           else region end region_c2
                           from R3