select phone,
       date(call_date)          calldate,
       substring(dialog, 11, 4) dialog,
       town_c,
       last_step,
       case
           when contacts_cstm.ptv_c like '%^3^%'
               or contacts_cstm.ptv_c like '%^5^%'
               or contacts_cstm.ptv_c like '%^6^%'
               or contacts_cstm.ptv_c like '%^10^%'
               or contacts_cstm.ptv_c like '%^11^%'
               or contacts_cstm.ptv_c like '%^19^%'
               or contacts_cstm.ptv_c like '%^14^%' then 'Разметка Наша'
           when contacts_cstm.ptv_c like '%^3_19^%'
               or contacts_cstm.ptv_c like '%^5_19^%'
               or contacts_cstm.ptv_c like '%^6_19^%'
               or contacts_cstm.ptv_c like '%^10_19^%'
               or contacts_cstm.ptv_c like '%^11_19^%'
               or contacts_cstm.ptv_c like '%^19_19^%'
               or contacts_cstm.ptv_c like '%^14_19^%' then 'Разметка не наша 50+'
           when
                       contacts_cstm.ptv_c like '%^3_21^%'
                   or contacts_cstm.ptv_c like '%^5_21^%'
                   or contacts_cstm.ptv_c like '%^6_21^%'
                   or contacts_cstm.ptv_c like '%^10_21^%'
                   or contacts_cstm.ptv_c like '%^11_21^%'
                   or contacts_cstm.ptv_c like '%^19_21^%'
                   or contacts_cstm.ptv_c like '%^14_21^%' then 'Разметка не наша Телеком'
           when
                       contacts_cstm.ptv_c like '%^3_18^%'
                   or contacts_cstm.ptv_c like '%^5_18^%'
                   or contacts_cstm.ptv_c like '%^6_18^%'
                   or contacts_cstm.ptv_c like '%^10_18^%'
                   or contacts_cstm.ptv_c like '%^11_18^%'
                   or contacts_cstm.ptv_c like '%^19_18^%'
                   or contacts_cstm.ptv_c like '%^14_18^%' then 'Разметка не наша 50-40'
           when
                       contacts_cstm.ptv_c like '%^5_20^%'
                   or contacts_cstm.ptv_c like '%^3_20^%'
                   or contacts_cstm.ptv_c like '%^6_20^%'
                   or contacts_cstm.ptv_c like '%^10_20^%'
                   or contacts_cstm.ptv_c like '%^11_20^%'
                   or contacts_cstm.ptv_c like '%^19_20^%'
                   or contacts_cstm.ptv_c like '%^14_20^%' then 'Разметка не наша Спутник'
           when
                       contacts_cstm.ptv_c like '%^3_17^%'
                   or contacts_cstm.ptv_c like '%^5_17^%'
                   or contacts_cstm.ptv_c like '%^6_17^%'
                   or contacts_cstm.ptv_c like '%^10_17^%'
                   or contacts_cstm.ptv_c like '%^11_17^%'
                   or contacts_cstm.ptv_c like '%^19_17^%'
                   or contacts_cstm.ptv_c like '%^14_17^%' then 'Разметка не наша 40-30'
           when
                       contacts_cstm.ptv_c like '%^5_16^%'
                   or contacts_cstm.ptv_c like '%^3_16^%'
                   or contacts_cstm.ptv_c like '%^6_16^%'
                   or contacts_cstm.ptv_c like '%^10_16^%'
                   or contacts_cstm.ptv_c like '%^11_16^%'
                   or contacts_cstm.ptv_c like '%^19_16^%'
                   or contacts_cstm.ptv_c like '%^14_16^%' then 'Разметка не наша 30-20'
           when
                       contacts_cstm.ptv_c like '%^5_15^%'
                   or contacts_cstm.ptv_c like '%^3_15^%'
                   or contacts_cstm.ptv_c like '%^6_15^%'
                   or contacts_cstm.ptv_c like '%^10_15^%'
                   or contacts_cstm.ptv_c like '%^11_15^%'
                   or contacts_cstm.ptv_c like '%^19_15^%'
                   or contacts_cstm.ptv_c like '%^14_15^%' then 'Разметка не наша 20-0'
           when contacts_cstm.region_c = 1 then 'Наша полная'
           when contacts_cstm.region_c = 2 then 'Наша неполная'
           when contacts_cstm.region_c = 4 then 'Фиас из разных источников'
           when contacts_cstm.region_c = 5 then 'Фиас до города'
           when contacts_cstm.region_c = 6 then 'Старый town_c'
           when contacts_cstm.region_c = 7 then 'Def code'
           else '' end          ptv
from suitecrm_robot.jc_robot_log
         left join suitecrm.contacts
                   on phone = contacts.phone_work
         left join suitecrm.contacts_cstm on contacts.id = contacts_cstm.id_c
where  date(call_date) = date(now())
  and last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')