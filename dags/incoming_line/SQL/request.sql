with contacts as (select phone_work,
                         if(city_c is null or city_c = '', concat(contacts_cstm.town_c, '_t'), city_c) as city,
                         town_c,
                         city_c
                  from suitecrm.contacts
                           left join suitecrm.contacts_cstm on id = id_c)

select Meets.*
from (
         select last_queue_c,
                date_entered,
                status,
                konva,
                rtkid,
                if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                  '')) <=
                   10,
                   concat(8,
                          replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                   concat(8,
                          right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                        ''), 10))) as phone_work,
                assigned_user_id,
                'RTK'                                 start_project,
                created_by
         from (SELECT distinct rtk.id                    rtkid,
                               rtk_cstm.last_queue_c,
                               date(rtk.date_entered) as date_entered,
                               rtk.status,
                               rtk.assigned_user_id,
                               case
                                   when rtk.status in ('Held', 'Active') then 1
                                   when rtk.status in ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                                   else 0
                                   end                   konva,
                               packet_service_c          tarif,
                               rtk.phone_work,
                               rtk.created_by
               FROM suitecrm.jc_meetings_rostelecom rtk
                        left join suitecrm.jc_meetings_rostelecom_cstm rtk_cstm on rtk.id = rtk_cstm.id_c
                        left join contacts on rtk.phone_work = contacts.phone_work
               WHERE date(rtk.date_entered) >= '2023-08-01'
                 AND (rtk.status <> 'Error' and rtk.status <> 'doubled' and
                      rtk.status <> 'change_flat')
                 and rtk.deleted = 0
              ) RTK
         union all
         select last_queue_c,
                date_entered,
                status,
                konva,
                rtkid,
                if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                  '')) <=
                   10,
                   concat(8,
                          replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                   concat(8,
                          right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                        ''), 10))) as phone_work,
                assigned_user_id,
                'BEELINE'                             start_project,
                created_by
         from (SELECT distinct bln.id                    rtkid,
                               bln_cstm.last_queue_c,
                               date(bln.date_entered) as date_entered,
                               bln.assigned_user_id,
                               bln.status,
                               case
                                   when bln.status in ('Held', 'Active', 'Proverka', 'delivered') then 1
                                   when bln.status in ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                                   else 0
                                   end                   konva,
                               bln.phone_work,
                               bln.created_by
               FROM suitecrm.jc_meetings_beeline bln
                        left join suitecrm.jc_meetings_beeline_cstm bln_cstm on bln.id = bln_cstm.id_c
                        left join contacts on bln.phone_work = contacts.phone_work
               WHERE date(bln.date_entered) >= '2023-08-01'
                 AND (bln.status <> 'Error' and bln.status <> 'doubled' and
                      bln.status <> 'change_flat')
                 and bln.deleted = 0) BLN
         union all
         select last_queue_c,
                date_entered,
                status,
                konva,
                rtkid,
                if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                  '')) <=
                   10,
                   concat(8,
                          replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                   concat(8,
                          right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                        ''), 10))) as phone_work,
                assigned_user_id,
                'DOMRU'                               start_project,
                DOM.created_by
         from (SELECT distinct dom.id                    rtkid,
                               dom.last_queue_c,
                               dom.assigned_user_id,
                               date(dom.date_entered) as date_entered,
                               dom.status,
                               case
                                   when dom.status in ('Held', 'Active') then 1
                                   when dom.status in ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                                   else 0
                                   end                   konva,
                               dom.phone_work,
                               dom.created_by
               FROM suitecrm.jc_meetings_domru dom
                        left join suitecrm.jc_meetings_domru_cstm dom_cstm on id_c = id
                        left join contacts on dom.phone_work = contacts.phone_work
               WHERE date(dom.date_entered) >= '2023-08-01'
                 and (dom.status <> 'Error' and dom.status <> 'doubled' and
                      dom.status <> 'change_flat')
                 and dom.deleted = 0) DOM
         union all
         select last_queue_c,
                date_entered,
                status,
                konva,
                rtkid,
                if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                  '')) <=
                   10,
                   concat(8,
                          replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                   concat(8,
                          right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                        ''), 10))) as phone_work,
                assigned_user_id,
                'TTK'                                 start_project,
                TTK.created_by
         from (SELECT distinct ttk.id                    rtkid,
                               ttk_cstm.last_queue_c,
                               ttk.assigned_user_id,
                               date(ttk.date_entered) as date_entered,
                               ttk.status,
                               case
                                   when ttk.status in ('Held', 'Active') then 1
                                   when ttk.status in ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                                   else 0
                                   end                   konva,
                               ttk.phone_work,
                               ttk.created_by
               FROM suitecrm.jc_meetings_ttk ttk
                        left join suitecrm.jc_meetings_ttk_cstm ttk_cstm on ttk.id = ttk_cstm.id_c
                        left join contacts
                                  on ttk.phone_work = contacts.phone_work
               WHERE date(ttk.date_entered) >= '2023-08-01'
                 AND (ttk.status <> 'Error' and ttk.status <> 'doubled' and
                      ttk.status <> 'change_flat')
                 and ttk.deleted = 0) TTK
         union all
         select last_queue_c,
                date_entered,
                status,
                konva,
                rtkid,
                if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                  '')) <=
                   10,
                   concat(8,
                          replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                   concat(8,
                          right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                        ''), 10))) as phone_work,
                assigned_user_id,
                'NBN'                                 start_project,
                NBN.created_by
         from (SELECT distinct nbn.id                    rtkid,
                               nbn_cstm.last_queue_c,
                               nbn.assigned_user_id,
                               date(nbn.date_entered) as date_entered,
                               nbn.status,
                               case
                                   when nbn.status in ('Held', 'Active') then 1
                                   when nbn.status in ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                                   else 0
                                   end                   konva,
                               nbn.phone_work,
                               nbn.created_by
               FROM suitecrm.jc_meetings_netbynet nbn
                        left join suitecrm.jc_meetings_netbynet_cstm nbn_cstm on nbn.id = nbn_cstm.id_c
                        left join contacts on nbn.phone_work = contacts.phone_work
               WHERE date(nbn.date_entered) >= '2023-08-01'
                 AND (nbn.status <> 'Error' and nbn.status <> 'doubled' and
                      nbn.status <> 'change_flat')
                 and nbn.deleted = 0) NBN
         union all
         select last_queue_c,
                date_entered,
                status,
                konva,
                rtkid,
                if(length(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                  '')) <=
                   10,
                   concat(8,
                          replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ', '')),
                   concat(8,
                          right(replace(replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''), ' ',
                                        ''), 10))) as phone_work,
                assigned_user_id,
                'MTS'                                 start_project,
                MTS.created_by
         from (SELECT distinct mts.id                    rtkid,
                               mts_cstm.last_queue_c,
                               mts.assigned_user_id,
                               date(mts.date_entered) as date_entered,
                               mts.status                status,
                               case
                                   when mts.status in ('Held', 'Active') then 1
                                   when mts.status in ('Created', 'provider_planned', 'dispetcher_grafik')
                                       then 2
                                   else 0
                                   end                   konva,
                               mts.phone_work,
                               mts.created_by
               FROM suitecrm.jc_meetings_mts mts
                        left join suitecrm.jc_meetings_mts_cstm mts_cstm on mts.id = mts_cstm.id_c
                        left join contacts on mts.phone_work = contacts.phone_work
               WHERE date(mts.date_entered) >= '2023-08-01'
                 and (mts.status <> 'Error' and mts.status <> 'doubled' and
                      mts.status <> 'change_flat')
                 and mts.deleted = 0) MTS
     ) Meets