with 
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
     teams as (select *,
                      REGEXP_SUBSTR(first_name, '[^0-9]') team
                         
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
     
                 
     JC as (  
            select distinct call_date,
                            robot_id         queue,
                            robot_id         Last_queue,
                            network_provider network_provider_c,
                            city             city_c,
                            quality region,
                            trunk_id,
                            ptv              ptv_c,
                            base_source      base_source_c,
                            robot_log_id     id,
                            billsec,
                            operator_id      assigned_user_id,
                            real_billsec,
                            robot_log_id     uniqueid,
                            phone,
                            marker,
                            last_step,
                            route,
                            voice            directory,
                            transfer destination_queue
            FROM suitecrm_robot.robot_log
                     left join suitecrm_robot.robot_log_addition on robot_log.id = robot_log_addition.robot_log_id
            WHERE date(call_date) = date(now()) - interval {n} day
     ),


     R as (SELECT date(call_date)                                                call_date,
                  queue,
                  Last_queue,
                  IF(destination_queue is null, queue,
                     destination_queue)                                          destination_queue,
                  case
                      when jc_robot_log.network_provider_c = '83' then 'МТС'
                      when jc_robot_log.network_provider_c = '80' then 'Билайн'
                      when jc_robot_log.network_provider_c = '82' then 'Мегафон'
                      when jc_robot_log.network_provider_c = '10' then 'Теле2'
                      when jc_robot_log.network_provider_c = '68' then 'Теле2'
                      else 'MVNO'
                      end                                                        network_provider,
                  if(jc_robot_log.city_c is null or jc_robot_log.city_c in ('', 0), concat(cm.town_c, '_t'),
                     jc_robot_log.city_c)                                     as city_c,
                  region,
                  trunk_id,
                  jc_robot_log.ptv_c,
                  jc_robot_log.base_source_c,
                  jc_robot_log.uniqueid                                          id,
                  billsec,
                  jc_robot_log.assigned_user_id,
                  real_billsec,
                  jc_robot_log.uniqueid,
                  jc_robot_log.phone,
                  marker,
                  last_step,
                  code_data,
                  CASE 
                        WHEN route like '%%262%%' THEN 1 
                        ELSE 0 
                    END AS inbound_call,
                  
                  directory
           FROM JC jc_robot_log
                    left join suitecrm.contacts c on c.phone_work = jc_robot_log.phone
                    left join suitecrm.contacts_cstm cm on cm.id_c = c.id
                    left join suitecrm.contacts_custom_fields_new ccf on ccf.id_custom = cm.id_c
                    left join suitecrm.users u on jc_robot_log.assigned_user_id = u.id
                    ),

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
                   if(last_step in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372') and billsec <= 2, 0,
                      billsec)                            as billsec,
                   if(last_step in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372') and
                      real_billsec <= 2, 0, real_billsec) as real_billsec,
                   team,
                   marker,
                   last_step,
                   code_data,
                   inbound_call,
                   directory,
                   'DR'                                      proect
            from R
                
                     left join teams on R.assigned_user_id = teams.id
     ),
     R3 as (select R2.*,
                   case
                       when base_source_c like '%301%' then '55_m'
                       when base_source_c like '%302%' then '18_m'
                       when base_source_c like '%303%' then '55_f'
                       when base_source_c like '%304%' then '18_f'
                       else '' end department

            from R2
                     left join department on R2.team = department.team)

select call_date,
       queue,
       destination_queue,
       assigned_user_id,
       network_provider,
       city_c,
       trunk_id,
       id,
       billsec,
       real_billsec,
       department,
       marker,
       team,
       directory,
       last_step,
       code_data,
       inbound_call,
       proect,
       ''                   data_type,
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
           else region end  region_c,
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

           else region end  region_c2
from R3
