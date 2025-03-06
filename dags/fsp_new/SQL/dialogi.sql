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
       if(jc2.city_c is null or jc2.city_c in ('', 0), concat(jc2.town, '_t'), jc2.city_c) as city_c,
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

           else region_c end                                                                  region_c2,
#        city_c,
       directory,
       trunk_id
#        if(datecall < '2023-04-01',city_c,city) as city_c
from (select distinct jc.uniqueid                                                                    id,
                      DATE(jc.call_date)                                                             datecall,
                      jc.assigned_user_id,
                      jc.phone,
                      jc.city_c,
                      jc.town,
                      if(jc.city_c is null or jc.city_c = '', concat(cm.town_c, '_t'), jc.city_c) as city,
                      case
                          when jc.network_provider_c = '83' then 'МТС'
                          when jc.network_provider_c = '80' then 'Билайн'
                          when jc.network_provider_c = '82' then 'Мегафон'
                          when jc.network_provider_c = '10' then 'Теле2'
                          when jc.network_provider_c = '68' then 'Теле2'
                          else 'MVNO'
                          end                                                                        network_provider,
                      jc.ptv_c,
                      jc.region_c,
                      jc.marker,
                      jc.last_step,
                      queue,
                      destination_queue,
                      was_repeat,
                      inbound_call,
                      jc.base_source_c,
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
                   REGEXP_REPLACE(dialog, '[^0-9]', '')                                                 as queue,
                   was_repeat,
                   inbound_call,
                   directory,
                   trunk_id,
                   base_source_c
            from suitecrm_robot.jc_robot_log
            WHERE date(call_date) >= '2025-01-01'
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
                   REGEXP_REPLACE(dialog, '[^0-9]', '')                                                 as queue,
                   was_repeat,
                   inbound_call,
                   directory,
                   trunk_id,
                   base_source_c
            from suitecrm_robot.jc_robot_log_2025_01 jc1
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
                   REGEXP_REPLACE(dialog, '[^0-9]', '')                                                 as queue,
                   was_repeat,
                   inbound_call,
                   directory,
                   trunk_id,
                   base_source_c
            from suitecrm_robot.jc_robot_log_2024_12 jc2
            WHERE jc2.assigned_user_id not in ('1', '')
       --      union all
       --      select uniqueid,
       --             call_date,
       --             assigned_user_id,
       --             phone,
       --             city_c,
       --             town,
       --             network_provider_c,
       --             ptv_c,
       --             region_c,
       --             marker,
       --             last_step,
       --             REGEXP_REPLACE(dialog, '[^0-9]', '')                                                 as queue,
       --             was_repeat,
       --             inbound_call,
       --             directory,
       --             trunk_id,
       --             base_source_c
       --      from suitecrm_robot.jc_robot_log_2024_11 jc3
       --      WHERE jc3.assigned_user_id not in ('1', '')
       --      union all
       --      select uniqueid,
       --             call_date,
       --             assigned_user_id,
       --             phone,
       --             city_c,
       --             town,
       --             network_provider_c,
       --             ptv_c,
       --             region_c,
       --             marker,
       --             last_step,
       --             REGEXP_REPLACE(dialog, '[^0-9]', '')                                                 as queue,
       --             was_repeat,
       --             inbound_call,
       --             directory,
       --             trunk_id,
       --             base_source_c
       -- --      from suitecrm_robot.jc_robot_log_2024_10 jc3
       --      WHERE jc3.assigned_user_id not in ('1', '')
            union all
            SELECT robot_log_id         uniqueid,
                   call_date,
                   operator_id      assigned_user_id,
                   phone,
                   city             city_c,
                   region           town,
                   network_provider network_provider_c,
                   ptv              ptv_c,
                   quality          region_c,
                   marker,
                   last_step,
                   robot_id         queue,
                   was_ptv          was_repeat,
                   direction        inbound_call,
                   voice            directory,
                   trunk_id,
                   base_source      base_source_c
            FROM suitecrm_robot.robot_log
                     left join suitecrm_robot.robot_log_addition on robot_log.id = robot_log_addition.robot_log_id
            WHERE date(call_date) >= '2025-01-01'
              and robot_log_addition.operator_id not in ('1', '')
           ) jc
               left join suitecrm.contacts c on c.phone_work = jc.phone
               left join suitecrm.contacts_cstm cm on cm.id_c = c.id
               left join (select distinct *
                          from suitecrm.transferred_to_other_queue
                          union all
                          select robot_id     dialog,
                                 transfer     destination_queue,
                                 call_date    date,
                                 robot_log_id uniqueid,
                                 phone
                          FROM suitecrm_robot.robot_log
                                   left join suitecrm_robot.robot_log_addition
                                             on robot_log.id = robot_log_addition.robot_log_id
                          where transfer is not null
                            and transfer != '') tr on jc.uniqueid = tr.uniqueid and tr.phone = jc.phone
      WHERE

jc.assigned_user_id not in ('1', '')) jc2


where last_step in ({})
