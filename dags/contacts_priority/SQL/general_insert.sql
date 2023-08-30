insert into suitecrm_robot_ch.contacts_temp
                    select phone_work,
                        mobile_def,
                        Days,
                        city_c,
                        town_c,
                        region_c,
                        network_provider,
                        agreed_rtk,
                        case
                            when general_stop = 0
                                and ntv_ptv = 0
                                and ntv_step = 0
                                and stop_dom = 0
                                and ptvdom = 1 then 1
                            else 0 end ptv_dom,
                        case
                            when general_stop = 0
                                and ntv_ptv = 0
                                and ntv_step = 0
                                and stop_rtk = 0
                                and ptvrtk = 1 then 1
                            else 0 end ptv_rtk,
                        case
                            when general_stop = 0
                                and ntv_ptv = 0
                                and ntv_step = 0
                                and stop_ttk = 0
                                and ptvttk = 1 then 1
                            else 0 end ptv_ttk,
                        case
                            when general_stop = 0
                                and ntv_ptv = 0
                                and ntv_step = 0
                                and stop_bln = 0
                                and ptvbln = 1 then 1
                            else 0 end ptv_bln,
                        case
                            when general_stop = 0
                                and ntv_ptv = 0
                                and ntv_step = 0
                                and stop_mts = 0
                                and ptvmts = 1 then 1
                            else 0 end ptv_mts,
                        case
                            when general_stop = 0
                                and ntv_ptv = 0
                                and ntv_step = 0
                                and stop_nbn = 0
                                and ptvnbn = 1 then 1
                            else 0 end ptv_nbn,

                        case
                            when general_stop = 0
                                and ntv_ptv = 0
                                and ntv_step = 0
                                and stop_dom = 0
                                and ptv_nasha = 0
                                and ptvfias_dom = 1 then 1
                            else 0 end ptv_fias_dom,
                        case
                            when general_stop = 0
                                and ntv_ptv = 0
                                and ntv_step = 0
                                and stop_rtk = 0
                                and ptv_nasha = 0
                                and ptvfias_rtk = 1 then 1
                            else 0 end ptv_fias_rtk,
                        case
                            when general_stop = 0
                                and ntv_ptv = 0
                                and ntv_step = 0
                                and stop_ttk = 0
                                and ptv_nasha = 0
                                and ptvfias_ttk = 1 then 1
                            else 0 end ptv_fias_ttk,
                        case
                            when general_stop = 0
                                and ntv_ptv = 0
                                and ntv_step = 0
                                and stop_bln = 0
                                and ptv_nasha = 0
                                and ptvfias_bln = 1 then 1
                            else 0 end ptv_fias_bln,
                        case
                            when general_stop = 0
                                and ntv_ptv = 0
                                and ntv_step = 0
                                and stop_mts = 0
                                and ptv_nasha = 0
                                and ptvfias_mts = 1 then 1
                            else 0 end ptv_fias_mts,
                        case
                            when general_stop = 0
                                and ntv_ptv = 0
                                and ntv_step = 0
                                and stop_nbn = 0
                                and ptv_nasha = 0
                                and ptvfias_nbn = 1 then 1
                            else 0 end ptv_fias_nbn,

                        stop_dom,
                        stop_rtk,
                        stop_ttk,
                        stop_bln,
                        stop_mts,
                        stop_nbn,
                        stop_status,
                        stop_otkaz,
                        general_stop,
                        ntv_ptv,
                        ntv_step

                    from (select phone_work,
                                toDate(now()) - toDate(last_call_c)                                                 Days,
                                if(phone_work like '89%', 1, 0)                                                     mobile_def,
                                if(priority1 not in ('dom', 'rtk', 'ttk', 'bln', 'nbn', 'mts'), Null, priority1) as priority1,
                                if(priority2 not in ('dom', 'rtk', 'ttk', 'bln', 'nbn', 'mts'), Null, priority2) as priority2,
                                city_c,
                                town_c,
                                region_c,
                                case
                                    when network_provider_c = '83' then 'МТС'
                                    when network_provider_c = '80' then 'Билайн'
                                    when network_provider_c = '82' then 'Мегафон'
                                    when network_provider_c = '10' then 'Теле2'
                                    when network_provider_c = '68' then 'Теле2'
                                    else '0'
                                    end                                                                             network_provider,
                                if((base_source_c like '%^88^%'
                                    or base_source_c like '%^87^%' or base_source_c like '%^85^%'
                                    or base_source_c like '%^90^%'
                                    or base_source_c like '%^100^%'
                                    or base_source_c like '%^89^%'), 1, 0)                                       as agreed_rtk,

                                if(((stoplist_c not like '%^s^%' and stoplist_c not like '%^sb^%' and stoplist_c not like '%^ao^%') or
                                    stoplist_c is NULL or stoplist_c = ''), 0, 1)                                   general_stop,
                                if(ptv_c not like '%^n^%', 0, 1)                                                    ntv_ptv,
                                if((step_c not in ('105', '106', '107', '318') or step_c is null), 0, 1)            ntv_step,

                                if(ptv_c like '%^3^%' or ptv_c like '%^5^%' or ptv_c like '%^6^%' or ptv_c like '%^10_%' or
                                    ptv_c like '%^11_%' or ptv_c like '%^19_%', 1, 0)                                ptv_nasha,

                                if(ptv_c like '%^3^%', 1, 0)                                                     as ptvdom,
                                if(ptv_c like '%^5^%', 1, 0)                                                     as ptvrtk,
                                if(ptv_c like '%^6^%', 1, 0)                                                     as ptvttk,
                                if(ptv_c like '%^10^%', 1, 0)                                                    as ptvbln,
                                if(ptv_c like '%^11^%', 1, 0)                                                    as ptvmts,
                                if(ptv_c like '%^19^%', 1, 0)                                                    as ptvnbn,

                                if(ptv_c like '%^3_%', 1, 0)                                                     as ptvfias_dom,
                                if(ptv_c like '%^5_%', 1, 0)                                                     as ptvfias_rtk,
                                if(ptv_c like '%^6_%', 1, 0)                                                     as ptvfias_ttk,
                                if(ptv_c like '%^10_%', 1, 0)                                                    as ptvfias_bln,
                                if(ptv_c like '%^11_%', 1, 0)                                                    as ptvfias_mts,
                                if(ptv_c like '%^19_%', 1, 0)                                                    as ptvfias_nbn,

                                if(stoplist_c like '%3^%', 1, 0)                                                 as stop_dom,
                                if(stoplist_c like '%5^%', 1, 0)                                                 as stop_rtk,
                                if(stoplist_c like '%6^%', 1, 0)                                                 as stop_ttk,
                                if(stoplist_c like '%10^%', 1, 0)                                                as stop_bln,
                                if(stoplist_c like '%11^%', 1, 0)                                                as stop_mts,
                                if(stoplist_c like '%19^%', 1, 0)                                                as stop_nbn,

                                if((contacts_status_c not in ('MeetingWait', '1', 'CallWait') or contacts_status_c is null), 0,
                                    1)                                                                               stop_status,
                                if((otkaz_c is null or otkaz_c not in ('otkaz_10', 'otkaz_8', 'otkaz_24', 'otkaz_28', 'otkaz23')), 0,
                                    1)                                                                               stop_otkaz
                        from suitecrm_robot_ch.contacts_cstm) contacts