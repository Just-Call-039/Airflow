with calls as (select date(cl.date_entered) as calldate,
                      DATE_FORMAT(DATE_ADD(cl.date_entered, INTERVAL IF(MINUTE(cl.date_entered) >= 58, 1, 0) HOUR),
                                  '%H')        hours,
                      cl_c.asterisk_caller_id_c,
                      cl_c.otkaz_c
               from suitecrm.calls as cl
                        left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
               where (date(cl.date_entered) = date(now()) - interval 1 day))

select substring(dialog, 11, 4)                                              dialog,
       phone,
       date(call_date)                                                       calldate,
       if(last_step in ('111', '0'), 1, 0)                                   autootvet,
       if(last_step not in ('', 0, 111, 371, 372, 362, 361, 261, 262), 1, 0) talk,
       last_step,
       trunk_id,
       marker,
       uniqueid,
       case
           when
               (ptv_c like '%^3^%'
                   or ptv_c like '%^5^%'
                   or ptv_c like '%^6^%'
                   or ptv_c like '%^10^%'
                   or ptv_c like '%^11^%'
                   or ptv_c like '%^19^%'
                   or ptv_c like '%^14^%') then 'Разметка Наша'
           when
               (ptv_c like '%^3_19^%'
                   or ptv_c like '%^5_19^%'
                   or ptv_c like '%^6_19^%'
                   or ptv_c like '%^10_19^%'
                   or ptv_c like '%^11_19^%'
                   or ptv_c like '%^19_19^%'
                   or ptv_c like '%^14_19^%') then 'Разметка не наша'
           when
               (ptv_c like '%^3_21^%'
                   or ptv_c like '%^5_21^%'
                   or ptv_c like '%^6_21^%'
                   or ptv_c like '%^10_21^%'
                   or ptv_c like '%^11_21^%'
                   or ptv_c like '%^19_21^%'
                   or ptv_c like '%^14_21^%') then 'Разметка не наша'
           when
               (ptv_c like '%^3_18^%'
                   or ptv_c like '%^5_18^%'
                   or ptv_c like '%^6_18^%'
                   or ptv_c like '%^10_18^%'
                   or ptv_c like '%^11_18^%'
                   or ptv_c like '%^19_18^%'
                   or ptv_c like '%^14_18^%') then 'Разметка не наша'
           when
               (ptv_c like '%^5_20^%'
                   or ptv_c like '%^3_20^%'
                   or ptv_c like '%^6_20^%'
                   or ptv_c like '%^10_20^%'
                   or ptv_c like '%^11_20^%'
                   or ptv_c like '%^19_20^%'
                   or ptv_c like '%^14_20^%') then 'Разметка не наша'
           when
               (ptv_c like '%^3_17^%'
                   or ptv_c like '%^5_17^%'
                   or ptv_c like '%^6_17^%'
                   or ptv_c like '%^10_17^%'
                   or ptv_c like '%^11_17^%'
                   or ptv_c like '%^19_17^%'
                   or ptv_c like '%^14_17^%') then 'Разметка не наша'
           when
               (ptv_c like '%^5_16^%'
                   or ptv_c like '%^3_16^%'
                   or ptv_c like '%^6_16^%'
                   or ptv_c like '%^10_16^%'
                   or ptv_c like '%^11_16^%'
                   or ptv_c like '%^19_16^%'
                   or ptv_c like '%^14_16^%') then 'Разметка не наша'
           when
               (ptv_c like '%^5_15^%'
                   or ptv_c like '%^3_15^%'
                   or ptv_c like '%^6_15^%'
                   or ptv_c like '%^10_15^%'
                   or ptv_c like '%^11_15^%'
                   or ptv_c like '%^19_15^%'
                   or ptv_c like '%^14_15^%') then 'Разметка не наша'

           when region_c = 1 then 'Наша полная'
           when region_c = 2 then 'Наша неполная'
           when region_c = 4 then 'Фиас из разных источников'
           when region_c = 5 then 'Фиас до города'
           when region_c = 6 then 'Старый town_c'
           when region_c = 7 then 'Def code'
           else '' end                                                       ptv_c,
       city_c,
       if(real_billsec is null, billsec, real_billsec)                       call_sec,
       if(otkaz = 'otkaz_23', 1, 0)                                          otkaz,
       1                                                                     calls
from suitecrm_robot.jc_robot_log
         left join calls on phone = asterisk_caller_id_c and calldate = date(call_date)
where date(call_date) = date(now()) - interval 1 day