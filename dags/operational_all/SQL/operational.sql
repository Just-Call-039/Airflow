with etv as (SELECT substring(turn, 11, 4)                                      as queue,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 1), ',', -1) as have_ptv_1,

                    case
                        when SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 1), ',', -1) =
                             SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 2), ',', -1)
                            then null
                        else SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 2), ',', -1)
                        end                                                     as have_ptv_2,

                    case
                        when SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 2), ',', -1) =
                             SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 3), ',', -1)
                            then null
                        else SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 3), ',', -1)
                        end                                                     as have_ptv_3,

                    case
                        when SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 3), ',', -1) =
                             SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 4), ',', -1)
                            then null
                        else SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 4), ',', -1)
                        end                                                     as have_ptv_4,

                    case
                        when SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 4), ',', -1) =
                             SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 5), ',', -1)
                            then null
                        else SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 5), ',', -1)
                        end                                                     as have_ptv_5,

                    case
                        when SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 5), ',', -1) =
                             SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 6), ',', -1)
                            then null
                        else SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 6), ',', -1)
                        end                                                     as have_ptv_6,

                    case
                        when SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 6), ',', -1) =
                             SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 7), ',', -1)
                            then null
                        else SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 7), ',', -1)
                        end                                                     as have_ptv_7
             FROM suitecrm.jc_robot_reportconfig
                      INNER JOIN suitecrm.jc_robot_reportconfig_cstm ON id = id_C
             WHERE deleted = 0
               and have_ptv is not null),
     config as (select D.name                                        '???????? ???????',
                       step.name                                     '???????? ???? ????????',
                       index_number                                  'step',
                       replace(replace(queue, '_NEW^', ''), '^', '') 'dialogs',
                       custom_queue_c                                '??????????? ???????',
                       case when action_type>0 and check_exit = 1 then '0'
                        when action_type>0 and check_exit = 0 then '1' end type_steps
                from suitecrm.jc_robconfig_step step
                         left join suitecrm.jc_robconfig_step_cstm cstm on step.id = cstm.id_c
                         left join suitecrm.jc_robconfig_dialog_jc_robconfig_step_c sv
                                   on step.id = sv.jc_robconfig_dialog_jc_robconfig_stepjc_robconfig_step_idb
                         left join suitecrm.jc_robconfig_dialog D
                                   on D.id = sv.jc_robconfig_dialog_jc_robconfig_stepjc_robconfig_dialog_ida
                where action_type > 0 ),
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
                        select step, queue_2,type_steps
                        from tabeue
                        union all
                        select step, queue_3,type_steps
                        from tabeue
                        union all
                        select step, queue_4,type_steps
                        from tabeue
                        union all
                        select step, queue_5,type_steps
                        from tabeue
                        union all
                        select step, queue_6,type_steps
                        from tabeue
                        union all
                        select step, queue_7,type_steps
                        from tabeue
                        union all
                        select step, queue_8,type_steps
                        from tabeue
                        union all
                        select step, queue_9,type_steps
                        from tabeue
                        union all
                        select step, queue_10,type_steps
                        from tabeue
                        union all
                        select step, queue_11,type_steps
                        from tabeue
                        union all
                        select step, queue_12,type_steps
                        from tabeue
                        union all
                        select step, queue_13,type_steps
                        from tabeue
                        union all
                        select step, queue_14,type_steps
                        from tabeue
                        union all
                        select step, queue_15,type_steps
                        from tabeue
                        union all
                        select step, queue_16,type_steps
                        from tabeue
                        union all
                        select step, queue_17,type_steps
                        from tabeue
                        union all
                        select step, queue_18,type_steps
                        from tabeue
                        union all
                        select step, queue_19,type_steps
                        from tabeue
                        union all
                        select step, queue_20,type_steps
                        from tabeue
                        union all
                        select step, queue_21,type_steps
                        from tabeue
                        union all
                        select step, queue_22,type_steps
                        from tabeue
                        union all
                        select step, queue_23,type_steps
                        from tabeue
                        union all
                        select step, queue_24,type_steps
                        from tabeue
                        union all
                        select step, queue_25,type_steps
                        from tabeue
                        union all
                        select step, queue_26,type_steps
                        from tabeue
                        union all
                        select step, queue_27,type_steps
                        from tabeue
                        union all
                        select step, queue_28,type_steps
                        from tabeue
                        union all
                        select step, queue_29,type_steps
                        from tabeue
                        union all
                        select step, queue_30,type_steps
                        from tabeue
                        union all
                        select step, queue_31,type_steps
                        from tabeue
                        union all
                        select step, queue_32,type_steps
                        from tabeue
                        union all
                        select step, queue_33,type_steps
                        from tabeue
                        union all
                        select step, queue_34,type_steps
                        from tabeue
                        union all
                        select step, queue_35,type_steps
                        from tabeue
                        union all
                        select step, queue_36,type_steps
                        from tabeue
                        union all
                        select step, queue_37,type_steps
                        from tabeue
                        union all
                        select step, queue_38,type_steps
                        from tabeue
                        union all
                        select step, queue_39,type_steps
                        from tabeue
                        union all
                        select step, queue_40,type_steps
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
     project as (select *
                  from (select queue,
                               project,
                               date,
                               (row_number() over (partition by queue order by date desc)) as rw
                        from suitecrm.queue_project
                        where date >= '2022-02-01') as tb1
                  where rw = 1
                  order by 3),
     jc_today as (select distinct substring(jc.dialog, 11, 4)                                           dialog,
                                  case
                                      when destination_queue is null then substring(jc.dialog, 11, 4)
                                      else destination_queue end                                        destination_queue,
                                  client_status,
#                                   case when destination_queue is null then 'end' else client_status end client_status,
                                  date(call_date)                                                       calldate,
                                  was_repeat,
                                  jc.phone,
                                  jc.uniqueid,
                                  marker,
                                  otkaz,
                                  route,
                                  jc.assigned_user_id,
                                  last_step,
                                  case when type_steps = 1  then 1 else 0 end                          perevod,
                                  case when type_steps = 0  then 1 else 0 end                          lids,
                                  jc.ptv_c,
                                  jc.region_c,
                                  if(jc.city_c is null or jc.city_c = '',concat(cstm.town_c,'_t'),jc.city_c) city_c,
                                  base_source_c,
                                  if(stoplist_c like '%^ao^%',1,0) autootvet,
                                  case when (base_source_c = '10' or base_source_c like '%^10^%') then '1'
                                   when (base_source_c is null and istochnik_combo_c = 'stretched') then '> 1' 
                                   else '' end stretched,
                                  case
                                  when base_source_c like '%^61^%' then 7 
                                  when base_source_c like '%^62^%' then 1
                                  when base_source_c like '%^60^%' then 0 else '' end category_stat,
                                  case when real_billsec is null then billsec else real_billsec end     trafic,
                                  trunk_id,
                                  case
                                        when base_source_c like '%^81^%' then 'Стоп-листы РТК'
                                        when base_source_c like '%^32^%' then 'Партнерская база РТК'
                                        when base_source_c like '%^83^%' then 'Октябрь`23 РТК'
                                        when base_source_c like '%^82^%' then 'Сентябрь`23 РТК'
                                        when base_source_c like '%^86^%' then 'Август`23 РТК'
                                        when base_source_c like '%^85^%' then 'Июль`23 РТК'
                                        when base_source_c like '%^87^%' then 'Июнь`23 РТК'
                                        when base_source_c like '%^88^%' then 'Май`23 РТК'
                                        when base_source_c like '%^89^%' then 'Апрель`23 РТК'
                                        when base_source_c like '%^90^%' then 'Март`23 РТК'
                                        when base_source_c like '%^100^%' then 'Февраль`23 РТК'
                                        when base_source_c like '%^180^%' then 'Январь`23 РТК'
                                        when base_source_c like '%^173^%' then 'Декабрь`22 РТК'
                                        when base_source_c like '%^172^%' then 'Ноябрь`22 РТК'
                                        when base_source_c like '%^179^%' then 'Октябрь`22 РТК'
                                        when base_source_c like '%^178^%' then 'Сентябрь`22 РТК'
                                  else '' end as source
#                   from (select distinct * from suitecrm_robot.jc_robot_log) jc
from suitecrm_robot.jc_robot_log jc
                           left join (select distinct * from suitecrm.transferred_to_other_queue) tr
                                     on (jc.uniqueid = tr.uniqueid and jc.phone = tr.phone)
                           left join steps on (ochered = substring(jc.dialog, 11, 4) and last_step = step)
                           left join suitecrm.contacts c on jc.phone = c.phone_work
                           left join suitecrm.contacts_cstm cstm on c.id = cstm.id_c
                  where date(call_date) >= date(now()) - 1
#                     and last_step not in (111, 1, '', 0, 261, 262)
             and (inbound_call = 0 or inbound_call = '')
     ),
     jc_today_1 as (select distinct jc_today.*,
                                    if(autootvet=1 and trafic < 3,0,trafic) trafic1,
     team,
                                    case
                       when team = 13 then 'RTK'
                       when team = 555 then 'BEELINE'
                       when team in (122, 432, 667) then 'BEELINE LIDS'
                       when team in (20, 24, 40, 62, 63, 90, 100, 502, 503, 504, 506, 507, 509, 510, 511, 512, 513)
                           then 'DOMRU LIDS'
                       when team in (15, 25, 27, 30) then 'MTS'
                       when team in
                            (6, 7, 18, 23, 55, 56, 57, 58, 64, 65, 73, 74, 76, 77, 81, 87, 91, 101, 102, 103, 104, 117,
                             202) then 'MTS LIDS'
                       when team in (8, 32) then 'NBN'
                       when team in (45, 46, 49, 53) then 'NBN LIDS'
                       when team in
                            (1, 2, 3, 5, 11, 11, 11, 16, 17, 21, 26, 26, 29, 31, 35, 37, 38, 43, 47, 48, 51, 54, 59, 60,
                             66, 67, 68, 69, 71, 78, 79, 83, 84, 85, 86, 89, 92, 93, 94, 95, 96, 97, 98, 99, 105, 106,
                             109, 113, 115, 116, 120, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 135, 136, 137,
                             138, 139, 140, 666, 689) then 'RTK LIDS'
                       when team in (14, 33, 34, 36, 39, 41, 44, 61, 118, 121, 123, 141) then 'TTK LIDS'

                       when destination_queue in
                            (9001, 9003, 9036, 9038, 9042, 9049, 9051, 9068, 9072, 9017, 9081, 9262, 9082, 9084, 9094,
                             9099, 9111, 9224) then 'BEELINE'
                       when destination_queue in
                            (9006, 9019, 9024, 9031, 9047, 9052, 9053, 9058, 9071, 9080, 9083, 9096, 9119, 9128, 9225,
                             9232, 9237, 9239, 9242, 9257, 9292) then 'DOMRU LIDS'
                       when destination_queue in
                            (9028, 9012, 9164, 9045, 9029, 9016, 9023, 9041, 9075, 9077, 9078, 9085, 9131, 9133, 9170,
                             9191, 9227, 9264, 9270, 9270, 9180) then 'MTS'
                       when destination_queue in
                            (9010, 9179, 9043, 9178, 9044, 0, 9048, 9073, 9184, 9106, 9120, 9121, 9137, 9141, 9267,
                             9142, 9263, 9143, 9021, 9145, 9173, 9177, 9208, 9169, 9210, 9185, 9176, 9211, 9220, 9138,
                             9221, 9162, 9174, 9222, 9223, 9717, 9226, 9229, 9228, 9196, 9230, 9140, 9240, 9241, 9251,
                             9252, 9263, 9267, 9268) then 'MTS LIDS'
                       when destination_queue in (9040, 9055, 9118, 9244, 9063) then 'NBN'
                       when destination_queue in
                            (9007, 9013, 9291, 9025, 9291, 9027, 9050, 9036, 9255, 9030, 9060, 9213, 9061, 9062, 9076,
                             9289, 9037, 9289, 9087, 9038, 9076, 9090, 9059, 9091, 9098, 9101, 9102, 9103, 9105, 9115,
                             9158, 9175, 9184, 9290, 9200, 9202, 9206, 9213, 9204, 9214, 9034, 9245, 9234, 9215, 9218,
                             9233, 9243, 9247, 9248, 9249, 9253, 9254, 9256) then 'RTK LIDS'
                       when destination_queue in
                            (9011, 9014, 9039, 9056, 9070, 9079, 9092, 9095, 9100, 9110, 9117, 9116, 9026, 9132, 9217,
                             9261, 9269, 9274, 9275, 9276, 9277, 9278, 9280, 9288) then 'TTK LIDS'
                       when destination_queue = 9057 then 'NBN LIDS'
                       when project.project = 10 then 'BEELINE'
                       when project.project = 11 then 'MTS'
                       when project.project = 19 then 'NBN'
                       when project.project = 3 then 'DOMRU LIDS'
                       when project.project = 6 then 'TTK LIDS'
                       when project.project = 5 then 'RTK LIDS'
                       when project.project = 12 then 'BEELINE (sim)'
                       when project.project = 13 then 'MTS (sim)'
                       else 'DR'
                       end
                       proect
                    from jc_today
                             left join project on destination_queue = project.queue
     left join teams on jc_today.assigned_user_id = teams.id
                    where trafic > 0),
     jc_today_2 as (select *,
                           case
                               when
                                   #                                    dialog in (9077, 9191, 9099, 9117, 9268, 9058, 9244, 9111, 9227, 9270, 9170, 9236,9095)
#                                    and
                                   (ptv_c like '%^3^%'
                                       or ptv_c like '%^5^%'
                                       or ptv_c like '%^6^%'
                                       or ptv_c like '%^10^%'
                                       or ptv_c like '%^11^%'
                                       or ptv_c like '%^19^%'
                                       or ptv_c like '%^14^%') then 'Разметка Наша'
                               when
                                   #                                    dialog in (9077, 9191, 9099, 9117, 9268, 9058, 9244, 9111, 9227, 9270, 9170)
#                                    and
                                   (ptv_c like '%^3_19^%'
                                       or ptv_c like '%^5_19^%'
                                       or ptv_c like '%^6_19^%'
                                       or ptv_c like '%^10_19^%'
                                       or ptv_c like '%^11_19^%'
                                       or ptv_c like '%^19_19^%'
                                       or ptv_c like '%^14_19^%') then 'Разметка не наша 50+'
                               when
                                   #                                    dialog in (9077, 9191, 9099, 9117, 9268, 9058, 9244, 9111, 9227, 9270, 9170)
#                                    and
                                   (ptv_c like '%^3_21^%'
                                       or ptv_c like '%^5_21^%'
                                       or ptv_c like '%^6_21^%'
                                       or ptv_c like '%^10_21^%'
                                       or ptv_c like '%^11_21^%'
                                       or ptv_c like '%^19_21^%'
                                       or ptv_c like '%^14_21^%') then 'Разметка не наша Телеком'
                               when
                                   #                                    dialog in (9077, 9191, 9099, 9117, 9268, 9058, 9244, 9111, 9227, 9270, 9170)
#                                    and
                                   (ptv_c like '%^3_18^%'
                                       or ptv_c like '%^5_18^%'
                                       or ptv_c like '%^6_18^%'
                                       or ptv_c like '%^10_18^%'
                                       or ptv_c like '%^11_18^%'
                                       or ptv_c like '%^19_18^%'
                                       or ptv_c like '%^14_18^%') then 'Разметка не наша 50-40'
                               when
                                   #                                    dialog in (9077, 9191, 9099, 9117, 9268, 9058, 9244, 9111, 9227, 9270, 9170)
#                                    and
                                   (ptv_c like '%^5_20^%'
                                       or ptv_c like '%^3_20^%'
                                       or ptv_c like '%^6_20^%'
                                       or ptv_c like '%^10_20^%'
                                       or ptv_c like '%^11_20^%'
                                       or ptv_c like '%^19_20^%'
                                       or ptv_c like '%^14_20^%') then 'Разметка не наша Спутник'
                               when
                                   #                                    dialog in (9077, 9191, 9099, 9117, 9268, 9058, 9244, 9111, 9227, 9270, 9170)
#                                    and
                                   (ptv_c like '%^3_17^%'
                                       or ptv_c like '%^5_17^%'
                                       or ptv_c like '%^6_17^%'
                                       or ptv_c like '%^10_17^%'
                                       or ptv_c like '%^11_17^%'
                                       or ptv_c like '%^19_17^%'
                                       or ptv_c like '%^14_17^%') then 'Разметка не наша 40-30'
                               when
                                   #                                    dialog in (9077, 9191, 9099, 9117, 9268, 9058, 9244, 9111, 9227, 9270, 9170)
#                                    and
                                   (ptv_c like '%^5_16^%'
                                       or ptv_c like '%^3_16^%'
                                       or ptv_c like '%^6_16^%'
                                       or ptv_c like '%^10_16^%'
                                       or ptv_c like '%^11_16^%'
                                       or ptv_c like '%^19_16^%'
                                       or ptv_c like '%^14_16^%') then 'Разметка не наша 30-20'
                               when
                                   #                                    dialog in (9077, 9191, 9099, 9117, 9268, 9058, 9244, 9111, 9227, 9270, 9170)
#                                    and
                                   (ptv_c like '%^5_15^%'
                                       or ptv_c like '%^3_15^%'
                                       or ptv_c like '%^6_15^%'
                                       or ptv_c like '%^10_15^%'
                                       or ptv_c like '%^11_15^%'
                                       or ptv_c like '%^19_15^%'
                                       or ptv_c like '%^14_15^%') then 'Разметка не наша 20-0'
                               when region_c = 1 then 'Наша полная'
                               when region_c = 2 then 'Наша неполная'
                               when region_c = 4 then 'Фиас из разных источников'
                               when region_c = 5 then 'Фиас до города'
                               when region_c = 6 then 'Старый town_c'
                               when region_c = 7 then 'Def code'
                               else '' end region
                    from jc_today_1),
     jc_today_3 as (select *,
                           case
                               when region in
                                    ('Наша полная', 'Наша неполная', 'Фиас из разных источников', 'Фиас до города')
                                   and base_source_c not like '%121%'
                                   and base_source_c not like '%122%'
                                   and base_source_c not like '%140%'
                                   and base_source_c not like '%119%'
                                   and base_source_c not like '%123%'
                                   and base_source_c not like '%142%'
                                   and base_source_c not like '%120%'
                                   and base_source_c not like '%124%'
                                   and base_source_c not like '%143%'
                                   and base_source_c not like '%127%'
                                   and base_source_c not like '%128%' then 'Холод'
                               when base_source_c not like '%122%'
                                   and base_source_c not like '%140%'
                                   and base_source_c not like '%119%'
                                   and base_source_c not like '%123%'
                                   and base_source_c not like '%142%'
                                   and base_source_c not like '%120%'
                                   and base_source_c not like '%124%'
                                   and base_source_c not like '%143%'
                                   and base_source_c not like '%127%'
                                   and base_source_c not like '%128%' then 'Холод'
                               else '' end holod
                    from jc_today_2)

select proect,
team,
       dialog,
       destination_queue,
       calldate,
       client_status,
       was_repeat,
       marker,
       route,
       otkaz,
       perevod,
       lids,
       1 calls,
       region,
       holod,
       city_c,
       trafic,
       trafic1,
       trunk_id,
       autootvet,
       category_stat,
       stretched,
       last_step,
       source,
       phone
       
from jc_today_3