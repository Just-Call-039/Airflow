with calls as (select jcrl.phone,
                      date(jcrl.call_date)          as call_date,
                      jcrl.route,
                      case
                          when
                                      jcrl.ptv_c like '%^3^%'
                                  or jcrl.ptv_c like '%^5^%'
                                  or jcrl.ptv_c like '%^6^%'
                                  or jcrl.ptv_c like '%^10^%'
                                  or jcrl.ptv_c like '%^11^%'
                                  or jcrl.ptv_c like '%^19^%'
                                  or jcrl.ptv_c like '%^14^%' then 'Разметка Наша'
                          when
                                      jcrl.ptv_c like '%^3_19^%'
                                  or jcrl.ptv_c like '%^5_19^%'
                                  or jcrl.ptv_c like '%^6_19^%'
                                  or jcrl.ptv_c like '%^10_19^%'
                                  or jcrl.ptv_c like '%^11_19^%'
                                  or jcrl.ptv_c like '%^19_19^%'
                                  or jcrl.ptv_c like '%^14_19^%' then 'Разметка не наша 50+'
                          when
                                      jcrl.ptv_c like '%^3_21^%'
                                  or jcrl.ptv_c like '%^5_21^%'
                                  or jcrl.ptv_c like '%^6_21^%'
                                  or jcrl.ptv_c like '%^10_21^%'
                                  or jcrl.ptv_c like '%^11_21^%'
                                  or jcrl.ptv_c like '%^19_21^%'
                                  or jcrl.ptv_c like '%^14_21^%' then 'Разметка не наша Телеком'
                          when
                                      jcrl.ptv_c like '%^3_18^%'
                                  or jcrl.ptv_c like '%^5_18^%'
                                  or jcrl.ptv_c like '%^6_18^%'
                                  or jcrl.ptv_c like '%^10_18^%'
                                  or jcrl.ptv_c like '%^11_18^%'
                                  or jcrl.ptv_c like '%^19_18^%'
                                  or jcrl.ptv_c like '%^14_18^%' then 'Разметка не наша 50-40'
                          when
                                      jcrl.ptv_c like '%^5_20^%'
                                  or jcrl.ptv_c like '%^3_20^%'
                                  or jcrl.ptv_c like '%^6_20^%'
                                  or jcrl.ptv_c like '%^10_20^%'
                                  or jcrl.ptv_c like '%^11_20^%'
                                  or jcrl.ptv_c like '%^19_20^%'
                                  or jcrl.ptv_c like '%^14_20^%' then 'Разметка не наша Спутник'
                          when
                                      jcrl.ptv_c like '%^3_17^%'
                                  or jcrl.ptv_c like '%^5_17^%'
                                  or jcrl.ptv_c like '%^6_17^%'
                                  or jcrl.ptv_c like '%^10_17^%'
                                  or jcrl.ptv_c like '%^11_17^%'
                                  or jcrl.ptv_c like '%^19_17^%'
                                  or jcrl.ptv_c like '%^14_17^%' then 'Разметка не наша 40-30'
                          when
                                      jcrl.ptv_c like '%^5_16^%'
                                  or jcrl.ptv_c like '%^3_16^%'
                                  or jcrl.ptv_c like '%^6_16^%'
                                  or jcrl.ptv_c like '%^10_16^%'
                                  or jcrl.ptv_c like '%^11_16^%'
                                  or jcrl.ptv_c like '%^19_16^%'
                                  or jcrl.ptv_c like '%^14_16^%' then 'Разметка не наша 30-20'
                          when
                                      jcrl.ptv_c like '%^5_15^%'
                                  or jcrl.ptv_c like '%^3_15^%'
                                  or jcrl.ptv_c like '%^6_15^%'
                                  or jcrl.ptv_c like '%^10_15^%'
                                  or jcrl.ptv_c like '%^11_15^%'
                                  or jcrl.ptv_c like '%^19_15^%'
                                  or jcrl.ptv_c like '%^14_15^%' then 'Разметка не наша 20-0'
                          else ''
                          end                       as ptv,
                      case
                          when jcrl.region_c = 1 then 'Наша полная'
                          when jcrl.region_c = 2 then 'Наша неполная'
                          when jcrl.region_c = 4 then 'Фиас из разных источников'
                          when jcrl.region_c = 5 then 'Фиас до города'
                          when jcrl.region_c = 6 then 'Старый town_c'
                          when jcrl.region_c = 7 then 'Def code'
                          else ''
                          end                       as region,
                      jcrl.last_step,
                      jcrl.uniqueid,
                      jcrl.client_status,
                      jcrl.otkaz,
                      jcrl.was_repeat,
                      substring(jcrl.dialog, 11, 4) as queue,
                      jcrl.server_number,
                      jcrl.directory,
                      case
                          when jcrl.city_c is null then concat(jcrl.town, 'OBL')
                          when jcrl.city_c in ('', ' ') then concat(jcrl.town, 'OBL')
                          else jcrl.city_c
                          end                       as city_c,
                      jcrl.region_c
               from suitecrm_robot.jc_robot_log as jcrl
               where date(jcrl.call_date) = date(now()) - interval 1 day
                 and jcrl.last_step not in ('0', '1', '111', '261', '262'))

select phone,
       call_date,
       route,
       if(ptv not in ('', ' '), ptv, region) as quality,
       last_step,
       uniqueid,
       client_status,
       otkaz,
       was_repeat,
       queue,
       server_number,
       directory,
       city_c,
       region_c
from calls;
