insert into suitecrm_robot_ch.data
select town_c,
       city_c,
       network_provider,
       region,
       general_stop,
       ntv_ptv,
       ntv_step,
       ptvdom,
       ptvrtk,
       ptvttk,
       ptvbln,
       ptvmts,
       ptvnbn,
       ptv2com,
       ptvfias_dom,
       ptvfias_rtk,
       ptvfias_ttk,
       ptvfias_bln,
       ptvfias_mts,
       ptvfias_nbn,
       ptvfias_2com,
       stop_connected_bln,
       stop_connected_mts,
       stop_connected_2com,
       stop_connected_nbn,
       stop_connected_dom,
       stop_connected_rtk,
       stop_connected_ttk,
       stop_ro_connected_bln,
       stop_ro_connected_mts,
       stop_ro_connected_2com,
       stop_ro_connected_nbn,
       stop_ro_connected_dom,
       stop_ro_connected_rtk,
       stop_ro_connected_ttk,
       stop_ntv_bln,
       stop_ntv_mts,
       stop_ntv_2com,
       stop_ntv_nbn,
       stop_ntv_dom,
       stop_ntv_rtk,
       stop_ntv_ttk,
       stop_ntv_operator_bln,
       stop_ntv_operator_mts,
       stop_ntv_operator_2com,
       stop_ntv_operator_nbn,
       stop_ntv_operator_dom,
       stop_ntv_operator_rtk,
       stop_ntv_operator_ttk,
       stop_stop_bln,
       stop_stop_mts,
       stop_stop_2com,
       stop_stop_nbn,
       stop_stop_dom,
       stop_stop_rtk,
       stop_stop_ttk,
       stop_status,
       stop_otkaz,
       city_name,
       town_name,
       region_name,
       agreed_rtk,
       ptv_nasha,
       ptv_ne_nasha,
       if(ptv_nasha = 1 or ptv_ne_nasha = 1,1,0) ptv,
       stop_dom,
       stop_rtk,
       stop_ttk,
       stop_bln,
       stop_mts,
       stop_nbn,
       stop_2com,
       alive,
       priority1,
       priority2,
       rest_days,


       count(id) contacts
from (
         select id,
--              if(phone_work like '89%', 1, 0)                                                         mobile_def,
                contacts_cstm.city_c                                                                 as city_c,
                contacts_cstm.town_c                                                                 as town_c,
                contacts_cstm.region_c                                                               as region,
                case
                    when network_provider_c = '83' then 'МТС'
                    when network_provider_c = '80' then 'Билайн'
                    when network_provider_c = '82' then 'Мегафон'
                    when network_provider_c = '10' then 'Теле2'
                    when network_provider_c = '68' then 'Теле2'
                    else 'MVNO'
                    end                                                                                 network_provider,

                if(((stoplist_c not like '%^s^%' and stoplist_c not like '%^sb^%' and stoplist_c not like '%^ao^%') or
                    stoplist_c is NULL or stoplist_c = ''), 0, 1)                                       general_stop,
                if(ptv_c not like '%^n^%', 0, 1)                                                        ntv_ptv,
                if((step_c not in ('105', '106', '107', '318') or step_c is null), 0, 1)                ntv_step,


                if(ptv_c like '%^3^%', 1, 0)                                                         as ptvdom,
                if(ptv_c like '%^5^%', 1, 0)                                                         as ptvrtk,
                if(ptv_c like '%^6^%', 1, 0)                                                         as ptvttk,
                if(ptv_c like '%^10^%', 1, 0)                                                        as ptvbln,
                if(ptv_c like '%^11^%', 1, 0)                                                        as ptvmts,
                if(ptv_c like '%^19^%', 1, 0)                                                        as ptvnbn,
                if(ptv_c like '%^14^%', 1, 0)                                                        as ptv2com,

                if(ptv_c like '%^3_15^%' or ptv_c like '%^3_16^%' or ptv_c like '%^3_17^%' or ptv_c like '%^3_18^%' or
                   ptv_c like '%^3_19^%' or ptv_c like '%^3_20^%' or ptv_c like '%^3_21^%', 1, 0)    as ptvfias_dom,
                if(ptv_c like '%^5_15^%' or ptv_c like '%^5_16^%' or ptv_c like '%^5_17^%' or ptv_c like '%^5_18^%' or
                   ptv_c like '%^5_19^%' or ptv_c like '%^5_20^%' or ptv_c like '%^5_21^%', 1, 0)    as ptvfias_rtk,
                if(ptv_c like '%^6_15^%' or ptv_c like '%^6_16^%' or ptv_c like '%^6_17^%' or ptv_c like '%^6_18^%' or
                   ptv_c like '%^6_19^%' or ptv_c like '%^6_20^%' or ptv_c like '%^6_21^%', 1, 0)    as ptvfias_ttk,
                if(ptv_c like '%^10_15^%' or ptv_c like '%^10_16^%' or ptv_c like '%^10_17^%' or
                   ptv_c like '%^10_18^%' or
                   ptv_c like '%^10_19^%' or ptv_c like '%^10_20^%' or ptv_c like '%^10_21^%', 1, 0) as ptvfias_bln,
                if(ptv_c like '%^11_15^%' or ptv_c like '%^11_16^%' or ptv_c like '%^11_17^%' or
                   ptv_c like '%^11_18^%' or
                   ptv_c like '%^11_19^%' or ptv_c like '%^11_20^%' or ptv_c like '%^11_21^%', 1, 0) as ptvfias_mts,
                if(ptv_c like '%^19_15^%' or ptv_c like '%^19_16^%' or ptv_c like '%^19_17^%' or
                   ptv_c like '%^19_18^%' or
                   ptv_c like '%^19_19^%' or ptv_c like '%^19_20^%' or ptv_c like '%^19_21^%', 1, 0) as ptvfias_nbn,
                if(ptv_c like '%^14_15^%' or ptv_c like '%^14_16^%' or ptv_c like '%^14_17^%' or
                   ptv_c like '%^14_18^%' or
                   ptv_c like '%^14_19^%' or ptv_c like '%^14_20^%' or ptv_c like '%^14_21^%', 1, 0) as ptvfias_2com,

                if(stoplist_c like '%^c10^%', 1, 0)                                                          stop_connected_bln,
                if(stoplist_c like '%^c11^%', 1, 0)                                                          stop_connected_mts,
                if(stoplist_c like '%^c14^%', 1, 0)                                                          stop_connected_2com,
                if(stoplist_c like '%^c19^%', 1, 0)                                                          stop_connected_nbn,
                if(stoplist_c like '%^c3^%', 1, 0)                                                           stop_connected_dom,
                if(stoplist_c like '%^c5^%', 1, 0)                                                           stop_connected_rtk,
                if(stoplist_c like '%^c6^%', 1, 0)                                                           stop_connected_ttk,
                if(stoplist_c like '%^cr10^%', 1, 0)                                                         stop_ro_connected_bln,
                if(stoplist_c like '%^cr11^%', 1, 0)                                                         stop_ro_connected_mts,
                if(stoplist_c like '%^cr14^%', 1, 0)                                                         stop_ro_connected_2com,
                if(stoplist_c like '%^cr19^%', 1, 0)                                                         stop_ro_connected_nbn,
                if(stoplist_c like '%^cr3^%', 1, 0)                                                          stop_ro_connected_dom,
                if(stoplist_c like '%^cr5^%', 1, 0)                                                          stop_ro_connected_rtk,
                if(stoplist_c like '%^cr6^%', 1, 0)                                                          stop_ro_connected_ttk,
                if(stoplist_c like '%^n10^%', 1, 0)                                                          stop_ntv_bln,
                if(stoplist_c like '%^n11^%', 1, 0)                                                          stop_ntv_mts,
                if(stoplist_c like '%^n14^%', 1, 0)                                                          stop_ntv_2com,
                if(stoplist_c like '%^n19^%', 1, 0)                                                          stop_ntv_nbn,
                if(stoplist_c like '%^n3^%', 1, 0)                                                           stop_ntv_dom,
                if(stoplist_c like '%^n5^%', 1, 0)                                                           stop_ntv_rtk,
                if(stoplist_c like '%^n6^%', 1, 0)                                                           stop_ntv_ttk,
                if(stoplist_c like '%^o10^%', 1, 0)                                                          stop_ntv_operator_bln,
                if(stoplist_c like '%^o11^%', 1, 0)                                                          stop_ntv_operator_mts,
                if(stoplist_c like '%^o14^%', 1, 0)                                                          stop_ntv_operator_2com,
                if(stoplist_c like '%^o19^%', 1, 0)                                                          stop_ntv_operator_nbn,
                if(stoplist_c like '%^o3^%', 1, 0)                                                           stop_ntv_operator_dom,
                if(stoplist_c like '%^o5^%', 1, 0)                                                           stop_ntv_operator_rtk,
                if(stoplist_c like '%^o6^%', 1, 0)                                                           stop_ntv_operator_ttk,
                if(stoplist_c like '%^s10^%', 1, 0)                                                          stop_stop_bln,
                if(stoplist_c like '%^s11^%', 1, 0)                                                          stop_stop_mts,
                if(stoplist_c like '%^s14^%', 1, 0)                                                          stop_stop_2com,
                if(stoplist_c like '%^s19^%', 1, 0)                                                          stop_stop_nbn,
                if(stoplist_c like '%^s3^%', 1, 0)                                                           stop_stop_dom,
                if(stoplist_c like '%^s5^%', 1, 0)                                                           stop_stop_rtk,
                if(stoplist_c like '%^s6^%', 1, 0)                                                           stop_stop_ttk,


                if((contacts_status_c not in ('MeetingWait', '1', 'CallWait') or contacts_status_c is null), 0,
                   1)                                                                                   stop_status,
                if((otkaz_c is null or otkaz_c not in ('otkaz_10', 'otkaz_8', 'otkaz_24', 'otkaz_28', 'otkaz23')), 0,
                   1)                                                                                   stop_otkaz,
                city_name,
                town_name,
                region_name,
                if((base_source_c like '%^83^%'
                    or base_source_c like '%^82^%'
                    or base_source_c like '%^86^%'
                    or base_source_c like '%^85^%'
                    or base_source_c like '%^87^%'
                    or base_source_c like '%^88^%'
                    or base_source_c like '%^89^%'
                    or base_source_c like '%^90^%'
                    or base_source_c like '%^100^%'
                    or base_source_c like '%^180^%'
                    or base_source_c like '%^173^%'
                    or base_source_c like '%^172^%'
                    or base_source_c like '%^179^%'
                    or base_source_c like '%^178^%'
                    or base_source_c like '32'
                    or base_source_c like '%^32^%'), 1, 0)                                           as agreed_rtk,


                if(ptv_c like '%^3^%' or ptv_c like '%^5^%' or ptv_c like '%^6^%' or ptv_c like '%^10^%' or
                   ptv_c like '%^14^%' or
                   ptv_c like '%^11^%' or ptv_c like '%^19^%', 1, 0)                                    ptv_nasha,

                if(ptv_c like '%^3_15^%' or ptv_c like '%^3_16^%' or ptv_c like '%^3_17^%' or ptv_c like '%^3_18^%' or ptv_c like '%^3_19^%' or ptv_c like '%^3_20^%' or ptv_c like '%^3_21^%'
                                or ptv_c like '%^5_15^%' or ptv_c like '%^5_16^%' or ptv_c like '%^5_17^%' or ptv_c like '%^5_18^%' or ptv_c like '%^5_19^%' or ptv_c like '%^5_20^%' or ptv_c like '%^5_21^%'
                                or ptv_c like '%^6_15^%' or ptv_c like '%^6_16^%' or ptv_c like '%^6_17^%' or ptv_c like '%^6_18^%' or ptv_c like '%^6_19^%' or ptv_c like '%^6_20^%' or ptv_c like '%^6_21^%'
                                or ptv_c like '%^10_15^%' or ptv_c like '%^10_16^%' or ptv_c like '%^10_17^%' or ptv_c like '%^10_18^%' or ptv_c like '%^10_19^%' or ptv_c like '%^10_20^%' or ptv_c like '%^10_21^%'
                                or ptv_c like '%^11_15^%' or ptv_c like '%^11_16^%' or ptv_c like '%^11_17^%' or ptv_c like '%^11_18^%' or ptv_c like '%^11_19^%' or ptv_c like '%^11_20^%' or ptv_c like '%^11_21^%'
                                or ptv_c like '%^14_15^%' or ptv_c like '%^14_16^%' or ptv_c like '%^14_17^%' or ptv_c like '%^14_18^%' or ptv_c like '%^14_19^%' or ptv_c like '%^14_20^%' or ptv_c like '%^14_21^%'
                                or ptv_c like '%^19_15^%' or ptv_c like '%^19_16^%' or ptv_c like '%^19_17^%' or ptv_c like '%^19_18^%' or ptv_c like '%^19_19^%' or ptv_c like '%^19_20^%' or ptv_c like '%^19_21^%',1,0) ptv_ne_nasha,


                if(stoplist_c like '%3^%', 1, 0)                                                     as stop_dom,
                if(stoplist_c like '%5^%', 1, 0)                                                     as stop_rtk,
                if(stoplist_c like '%6^%', 1, 0)                                                     as stop_ttk,
                if(stoplist_c like '%10^%', 1, 0)                                                    as stop_bln,
                if(stoplist_c like '%11^%', 1, 0)                                                    as stop_mts,
                if(stoplist_c like '%19^%', 1, 0)                                                    as stop_nbn,
                if(stoplist_c like '%14^%', 1, 0)                                                    as stop_2com,


                case
                    when (base_source_c like '%^60^%'
                        or base_source_c like '%^61^%'
                        or base_source_c like '%^62^%'
                        or base_source_c like '%^31^%'
                        or base_source_c like '62'
                        or base_source_c like '31'
                        or base_source_c like '32'
                        or base_source_c like '%^32^%') and base_source_c not like '%^52^%' then 1
                    else 0 end                                                                          alive,
                    priority1,
                    priority2,

                    if(last_call_c is null or last_call_c > now() + interval 1 day ,20000, toDate(now()) - toDate(last_call_c)) rest_days


         from suitecrm_robot_ch.contacts_cstm
                  left join suitecrm_robot_ch.directory_city on directory_city.city_c = contacts_cstm.city_c
                  left join suitecrm_robot_ch.directory_town on directory_town.town_c = contacts_cstm.town_c
                  left join suitecrm_robot_ch.directory_region on directory_region.region_c = contacts_cstm.region_c
         ) rr
group by town_c,
         city_c,
         network_provider,
         region,
         general_stop,
         ntv_ptv,
         ntv_step,
         ptvdom,
         ptvrtk,
         ptvttk,
         ptvbln,
         ptvmts,
         ptvnbn,
         ptv2com,
         ptvfias_dom,
         ptvfias_rtk,
         ptvfias_ttk,
         ptvfias_bln,
         ptvfias_mts,
         ptvfias_nbn,
         ptvfias_2com,
         stop_connected_bln,
         stop_connected_mts,
         stop_connected_2com,
         stop_connected_nbn,
         stop_connected_dom,
         stop_connected_rtk,
         stop_connected_ttk,
         stop_ro_connected_bln,
         stop_ro_connected_mts,
         stop_ro_connected_2com,
         stop_ro_connected_nbn,
         stop_ro_connected_dom,
         stop_ro_connected_rtk,
         stop_ro_connected_ttk,
         stop_ntv_bln,
         stop_ntv_mts,
         stop_ntv_2com,
         stop_ntv_nbn,
         stop_ntv_dom,
         stop_ntv_rtk,
         stop_ntv_ttk,
         stop_ntv_operator_bln,
         stop_ntv_operator_mts,
         stop_ntv_operator_2com,
         stop_ntv_operator_nbn,
         stop_ntv_operator_dom,
         stop_ntv_operator_rtk,
         stop_ntv_operator_ttk,
         stop_stop_bln,
         stop_stop_mts,
         stop_stop_2com,
         stop_stop_nbn,
         stop_stop_dom,
         stop_stop_rtk,
         stop_stop_ttk,
         stop_status,
         stop_otkaz,
         city_name,
         town_name,
         region_name,
         agreed_rtk,
         ptv_nasha,
         ptv_ne_nasha,
         ptv,
         stop_dom,
         stop_rtk,
         stop_ttk,
         stop_bln,
         stop_mts,
         stop_nbn,
         stop_2com,
         alive,
         priority1,
         priority2,
         rest_days