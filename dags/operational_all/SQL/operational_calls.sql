with project as (select queue, project
                 from (select queue, project, row_number() over (partition by queue) rw
                       from (select queue,
#                               date,
                                    case
                                        when queue = 9128 then 'DOMRU Dop'
                                        when queue in (9020, 9133, 9024, 9047, 9041, 9043) then 'MGTS'
                                        when project = 11 and project_type = 1 then 'MTS LIDS'
                                        when project = 11 then 'MTS'
                                        when project = 10 and project_type = 1 then 'BEELINE LIDS'
                                        when project = 10 then 'BEELINE'
                                        when project = 19 and project_type = 1 then 'NBN LIDS'
                                        when project = 19 then 'NBN'
                                        when project = 3 and project_type = 1 then 'DOMRU LIDS'
                                        when project = 3 then 'DOMRU'
                                        when project = 5 and project_type = 1 then 'RTK LIDS'
                                        when project = 5 then 'RTK'
                                        when project = 6 and project_type = 1 then 'TTK LIDS'
                                        when project = 6 then 'TTK'
                                        else 'DR' end project
                             from suitecrm.queue_project
                             where date > date(now()) - 3) t) t2
                 where rw = 1),
     jc_today as (select last_queue_c,
                         id_c,
                         phone_work,
                         town_c,
                         city_c,
                         case when step_c not in ('', 111,261,262,1,0,361,362,371,372) then 1
                         when contacts_status_c in ('refusing', 'MeetingWait', 'Wait', 'CallWait') and last_queue_c = 9091 then 1
                         else 0 end talk,
                         case
                             when step_c not in ('', 111,261,262,1,0,361,362,371,372) and
                                  contacts_status_c in ('refusing', 'MeetingWait', 'Wait', 'CallWait') then 1
                             when contacts_status_c in ('refusing', 'MeetingWait', 'Wait', 'CallWait') and last_queue_c = 9091 then 1
                             else 0 end                                                      perevod,
                         case
                             when step_c not in ('', 111,261,262,1,0,361,362,371,372) and contacts_status_c = 'MeetingWait' then 1
                             when contacts_status_c = 'MeetingWait' and last_queue_c = 9091 then 1
                             else 0 end                                                      meeting,
                         marker_c,
                         count_good_calls_c,
                         date(last_call_c)                                                   calldate,
                         hour(last_call_c)                                                   callhour,
                         ptv_c,region_c,
                         base_source_c,
                         network_provider_c,
                         case
                             when base_source_c like '%^60^%' then 0
                             when base_source_c like '%^62^%' then 1
                             when base_source_c like '%^61^%' then 7
                             else '' end                                                     category,
                         if(stoplist_c like '%^ao^%', 1, 0)                                  stop_auto,
                         project.project
                  from suitecrm.contacts_cstm
                  left join suitecrm.contacts on id_c = id
                           left join project on queue = last_queue_c
                  where date(last_call_c) = date(now())),
     jc_today1 as (select *,
                        --   case
                        --       when (ptv_c like '%^3^%'
                        --           or ptv_c like '%^5^%'
                        --           or ptv_c like '%^6^%'
                        --           or ptv_c like '%^10^%'
                        --           or ptv_c like '%^11^%'
                        --           or ptv_c like '%^19^%') then 'Разметка Наша'
                        --       when (ptv_c like '%^3_19^%'
                        --           or ptv_c like '%^3_18^%'
                        --           or ptv_c like '%^3_21^%'
                        --           or ptv_c like '%^3_17^%'
                        --           or ptv_c like '%^3_20^%'
                        --           or ptv_c like '%^3_16^%'
                        --           or ptv_c like '%^3_15^%'
                        --           or ptv_c like '%^5_19^%'
                        --           or ptv_c like '%^5_18^%'
                        --           or ptv_c like '%^5_21^%'
                        --           or ptv_c like '%^5_17^%'
                        --           or ptv_c like '%^5_20^%'
                        --           or ptv_c like '%^5_16^%'
                        --           or ptv_c like '%^5_15^%'
                        --           or ptv_c like '%^6_19^%'
                        --           or ptv_c like '%^6_18^%'
                        --           or ptv_c like '%^6_21^%'
                        --           or ptv_c like '%^6_17^%'
                        --           or ptv_c like '%^6_20^%'
                        --           or ptv_c like '%^6_16^%'
                        --           or ptv_c like '%^6_15^%'
                        --           or ptv_c like '%^10_19^%'
                        --           or ptv_c like '%^10_18^%'
                        --           or ptv_c like '%^10_21^%'
                        --           or ptv_c like '%^10_17^%'
                        --           or ptv_c like '%^10_20^%'
                        --           or ptv_c like '%^10_16^%'
                        --           or ptv_c like '%^10_15^%'
                        --           or ptv_c like '%^11_19^%'
                        --           or ptv_c like '%^11_18^%'
                        --           or ptv_c like '%^11_21^%'
                        --           or ptv_c like '%^11_17^%'
                        --           or ptv_c like '%^11_20^%'
                        --           or ptv_c like '%^11_16^%'
                        --           or ptv_c like '%^11_15^%'
                        --           or ptv_c like '%^19_19^%'
                        --           or ptv_c like '%^19_18^%'
                        --           or ptv_c like '%^19_21^%'
                        --           or ptv_c like '%^19_17^%'
                        --           or ptv_c like '%^19_20^%'
                        --           or ptv_c like '%^19_16^%'
                        --           or ptv_c like '%^19_15^%') then 'Разметка не Наша'
                        --       when (base_source_c like '%^119^%' or base_source_c like '%^120^%' or
                        --             base_source_c like '%^121^%' or base_source_c like '%^122^%' or
                        --             base_source_c like '%^123^%' or base_source_c like '%^124^%' or
                        --             base_source_c like '%^127^%' or base_source_c like '%^128^%')
                        --           then 'Был на операторе'
                        --       when (ptv_c like '%^11_c^%'
                        --           or ptv_c like '%^6_c^%'
                        --           or ptv_c like '%^10_c^%') then 'Запланированный холод'
                        --       when (ptv_c like '%^6_c1^%'
                        --           or ptv_c like '%^10_c1^%'
                        --           or ptv_c like '%^11_c1^%') then '����� 120 ����'
                        --       else 'Холод' end data,
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
                                       or ptv_c like '%^14_19^%') then 'Разметка не наша 50+'
                               when
                                   (ptv_c like '%^3_21^%'
                                       or ptv_c like '%^5_21^%'
                                       or ptv_c like '%^6_21^%'
                                       or ptv_c like '%^10_21^%'
                                       or ptv_c like '%^11_21^%'
                                       or ptv_c like '%^19_21^%'
                                       or ptv_c like '%^14_21^%') then 'Разметка не наша Телеком'
                               when
                                   (ptv_c like '%^3_18^%'
                                       or ptv_c like '%^5_18^%'
                                       or ptv_c like '%^6_18^%'
                                       or ptv_c like '%^10_18^%'
                                       or ptv_c like '%^11_18^%'
                                       or ptv_c like '%^19_18^%'
                                       or ptv_c like '%^14_18^%') then 'Разметка не наша 50-40'
                               when
                                   (ptv_c like '%^5_20^%'
                                       or ptv_c like '%^3_20^%'
                                       or ptv_c like '%^6_20^%'
                                       or ptv_c like '%^10_20^%'
                                       or ptv_c like '%^11_20^%'
                                       or ptv_c like '%^19_20^%'
                                       or ptv_c like '%^14_20^%') then 'Разметка не наша Спутник'
                               when
                                   (ptv_c like '%^3_17^%'
                                       or ptv_c like '%^5_17^%'
                                       or ptv_c like '%^6_17^%'
                                       or ptv_c like '%^10_17^%'
                                       or ptv_c like '%^11_17^%'
                                       or ptv_c like '%^19_17^%'
                                       or ptv_c like '%^14_17^%') then 'Разметка не наша 40-30'
                               when
                                   (ptv_c like '%^5_16^%'
                                       or ptv_c like '%^3_16^%'
                                       or ptv_c like '%^6_16^%'
                                       or ptv_c like '%^10_16^%'
                                       or ptv_c like '%^11_16^%'
                                       or ptv_c like '%^19_16^%'
                                       or ptv_c like '%^14_16^%') then 'Разметка не наша 30-20'
                               when
                                   (ptv_c like '%^5_15^%'
                                       or ptv_c like '%^3_15^%'
                                       or ptv_c like '%^6_15^%'
                                       or ptv_c like '%^10_15^%'
                                       or ptv_c like '%^11_15^%'
                                       or ptv_c like '%^19_15^%'
                                       or ptv_c like '%^14_15^%') then 'Разметка не наша 20-0'

                               when region_c = 1 then 'Наша полная'
                               when region_c = 2 then 'Наша неполная'
                               when region_c = 4 then 'Фиас из разных источников'
                               when region_c = 5 then 'Фиас до города'
                               when region_c = 6 then 'Старый town_c'
                               when region_c = 7 then 'Def code'
                               else '' end data,
                          case
                              when network_provider_c = '83' then 'МТС'
                              when network_provider_c = '80' then 'Билайн'
                              when network_provider_c = '82' or network_provider_c = '63' then 'Мегафон'
                              when network_provider_c = '10' then 'Теле2'
                              when network_provider_c = '68' then 'Теле2'
                              else 'MVNO'
                              end              network_provider
                   from jc_today),
          campains as (select adial_campaign_contactscontacts_idb contact, queuenum_custom_c
                  from suitecrm.adial_campaign_contacts_c
                           left join suitecrm.adial_campaign_cstm
                                     on adial_campaign_contactsadial_campaign_ida = adial_campaign_cstm.id_c),
     jc_today2 as (select project,
                          calldate,
                          callhour,
                          network_provider,
                          count_good_calls_c,
                          data,
                          last_queue_c,
                          if(queuenum_custom_c is not null and queuenum_custom_c != '' and queuenum_custom_c != 'default', queuenum_custom_c, last_queue_c) custom_queue_c,
                          marker_c,
                          town_c,
                          city_c,
                          category,
                          stop_auto,
                          talk,
                          id_c,
                          phone_work,
                          perevod,
                          meeting
                   from jc_today1
                            left join campains on id_c = contact)


-- select project,
--        calldate,
--        callhour,
--        network_provider,
--        count_good_calls_c,
--        data as      'База',
--        last_queue_c,
--        marker_c,
--        town_c,
--        city_c,
--        category,
--        stop_auto,
--        sum(talk)    'Разговоры',
--        count(id_c)  'Звонки',
--        sum(perevod) 'Переводы',
--        sum(meeting) 'Заявки'
-- from jc_today2
-- group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

select project,
       calldate,
       callhour,
       network_provider,
       count_good_calls_c,
       data,
       last_queue_c,
       custom_queue_c,
       marker_c,
       town_c,
       city_c,
       category,
       stop_auto,
       talk,
       phone_work,
       id_c,
       perevod,
       meeting
from jc_today2