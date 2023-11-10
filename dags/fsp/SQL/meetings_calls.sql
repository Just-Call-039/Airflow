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
       --  if(datecall < '2023-04-01',city_c,city) as city_c
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

where last_step in ({})
--  or inbound_call = 1