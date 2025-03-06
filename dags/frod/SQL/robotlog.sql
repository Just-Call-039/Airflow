SELECT 
        id_c,
        phone,
        call_date,
        dialog,
        inbound_call,
        route,
        last_step,
        client_status,
        city,
        marker,
        town,
        ptv_c ptv,
        region_c region,
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

        else region_c end region2,
        billsec

from 

(SELECT 
        contact_id_c id_c,
        phone,
        call_date,
        REGEXP_SUBSTR(dialog, '[0-9]+') dialog,
        inbound_call,
        route,
        last_step,
        client_status,
        city_c city,
        marker,
        town,
        ptv_c,
        region_c,
        base_source_c,
        billsec

                         
   FROM suitecrm_robot.jc_robot_log
   WHERE date(call_date) = date(now()) - interval 1 day
  
  UNION ALL

  SELECT 

        contact_id as id_c,
        phone,
        call_date,
        robot_id as dialog,
        direction as inbound_call,
        route,
        last_step,
        client_status,
        city,
        marker,
        region as town,
        ptv as ptv_c,
        quality as region_c,
        base_source as base_source_c,
        billsec

    FROM suitecrm_robot.robot_log 
        left join suitecrm_robot.robot_log_addition 
            on robot_log.id = robot_log_addition.robot_log_id
    
   WHERE date(call_date) = date(now()) - interval 1 day) rl