 SELECT 
        id_c,
        phone_work phone,
        (last_call_c - interval 3 hour) as last_call_c,
        last_queue_c,
        step_c,
        contacts_status_c,
        marker_c, 
        ptv_c,
        town_c,
        region_c,
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

        else region_c end region2_c
                    
   FROM suitecrm.contacts_cstm
   LEFT JOIN suitecrm.contacts 
        ON id_c = id

  WHERE date(last_call_c) = date(now()) - interval 1 day
        AND (last_queue_c = '9242' OR last_queue_c = '9224')