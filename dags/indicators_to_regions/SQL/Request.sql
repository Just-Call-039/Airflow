select userid,
       dateentered,
       contact,
       statused,
       queue
from (select jc_meetings_beeline.assigned_user_id   userid,
             case
                 when jc_meetings_beeline.status in ('Held', 'Active') then 1
                 when jc_meetings_beeline.status in
                      ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                 else 0
                 end                                statused,
             date(jc_meetings_beeline.date_entered) dateentered,
             if(length(replace(replace(replace(replace(jc_meetings_beeline.phone_work, '-', ''), ')', ''), '(', ''),
                               ' ',
                               '')) <=
                10,
                concat(8,
                       replace(replace(replace(replace(jc_meetings_beeline.phone_work, '-', ''), ')', ''), '(', ''),
                               ' ', '')),
                concat(8,
                       right(replace(replace(replace(replace(jc_meetings_beeline.phone_work, '-', ''), ')', ''), '(',
                                             ''), ' ',
                                     ''), 10))) as  contact,
             jc_meetings_beeline_cstm.last_queue_c  queue
      from jc_meetings_beeline
               left join jc_meetings_beeline_cstm on id = id_c
               left join users on users.id = assigned_user_id
      where date(jc_meetings_beeline.date_entered) >= '2023-06-01'
        and (jc_meetings_beeline.status <> 'Error' and jc_meetings_beeline.status <> 'doubled' and
             jc_meetings_beeline.status <> 'change_flat')#beeline
      union all
      select jc_meetings_mts.assigned_user_id      userid,
             case
                 when jc_meetings_mts.status in ('Held', 'Active') then 1
                 when jc_meetings_mts.status in
                      ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                 else 0
                 end                               statused,
             date(jc_meetings_mts.date_entered)    dateentered,
             if(length(replace(replace(replace(replace(jc_meetings_mts.phone_work, '-', ''), ')', ''), '(', ''), ' ',
                               '')) <=
                10,
                concat(8,
                       replace(replace(replace(replace(jc_meetings_mts.phone_work, '-', ''), ')', ''), '(', ''), ' ',
                               '')),
                concat(8,
                       right(replace(replace(replace(replace(jc_meetings_mts.phone_work, '-', ''), ')', ''), '(', ''),
                                     ' ',
                                     ''), 10))) as contact,
             jc_meetings_mts_cstm.last_queue_c     queue
      from jc_meetings_mts
               left join jc_meetings_mts_cstm on id = id_c
               left join users on users.id = assigned_user_id
      where date(jc_meetings_mts.date_entered) >= '2023-06-01'
        and (jc_meetings_mts.status <> 'Error' and jc_meetings_mts.status <> 'doubled' and
             jc_meetings_mts.status <> 'change_flat')#MTS
      union all
      select jc_meetings_rostelecom.assigned_user_id   userid,
             case
                 when jc_meetings_rostelecom.status in ('Held', 'Active') then 1
                 when jc_meetings_rostelecom.status in
                      ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                 else 0
                 end                                   statused,
             date(jc_meetings_rostelecom.date_entered) dateentered,
             if(length(replace(replace(replace(replace(jc_meetings_rostelecom.phone_work, '-', ''), ')', ''), '(', ''),
                               ' ',
                               '')) <=
                10,
                concat(8,
                       replace(replace(replace(replace(jc_meetings_rostelecom.phone_work, '-', ''), ')', ''), '(', ''),
                               ' ', '')),
                concat(8,
                       right(replace(replace(replace(replace(jc_meetings_rostelecom.phone_work, '-', ''), ')', ''), '(',
                                             ''), ' ',
                                     ''), 10))) as     contact,
             jc_meetings_rostelecom_cstm.last_queue_c  queue
      from jc_meetings_rostelecom
               left join jc_meetings_rostelecom_cstm on id = id_c
               left join users on users.id = assigned_user_id
      where date(jc_meetings_rostelecom.date_entered) >= '2023-06-01'
        and (jc_meetings_rostelecom.status <> 'Error' and jc_meetings_rostelecom.status <> 'doubled' and
             jc_meetings_rostelecom.status <> 'change_flat')#RTK
      union all
      select jc_meetings_ttk.assigned_user_id      userid,
             case
                 when jc_meetings_ttk.status in ('Held', 'Active') then 1
                 when jc_meetings_ttk.status in
                      ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                 else 0
                 end                               statused,
             date(jc_meetings_ttk.date_entered)    dateentered,
             if(length(replace(replace(replace(replace(jc_meetings_ttk.phone_work, '-', ''), ')', ''), '(', ''), ' ',
                               '')) <=
                10,
                concat(8,
                       replace(replace(replace(replace(jc_meetings_ttk.phone_work, '-', ''), ')', ''), '(', ''), ' ',
                               '')),
                concat(8,
                       right(replace(replace(replace(replace(jc_meetings_ttk.phone_work, '-', ''), ')', ''), '(', ''),
                                     ' ',
                                     ''), 10))) as contact,
             jc_meetings_ttk_cstm.last_queue_c     queue
      from jc_meetings_ttk
               left join jc_meetings_ttk_cstm on id = id_c
               left join users on users.id = assigned_user_id
      where date(jc_meetings_ttk.date_entered) >= '2023-06-01'
        and (jc_meetings_ttk.status <> 'Error' and jc_meetings_ttk.status <> 'doubled' and
             jc_meetings_ttk.status <> 'change_flat')#TTK
      union all
      select jc_meetings_domru.assigned_user_id    userid,
             case
                 when jc_meetings_domru.status in ('Held', 'Active') then 1
                 when jc_meetings_domru.status in
                      ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                 else 0
                 end                               statused,
             date(jc_meetings_domru.date_entered)  dateentered,
             if(length(replace(replace(replace(replace(jc_meetings_domru.phone_work, '-', ''), ')', ''), '(', ''), ' ',
                               '')) <=
                10,
                concat(8,
                       replace(replace(replace(replace(jc_meetings_domru.phone_work, '-', ''), ')', ''), '(', ''), ' ',
                               '')),
                concat(8,
                       right(replace(replace(replace(replace(jc_meetings_domru.phone_work, '-', ''), ')', ''), '(', ''),
                                     ' ',
                                     ''), 10))) as contact,
             jc_meetings_domru.last_queue_c        queue
      from jc_meetings_domru
               left join jc_meetings_domru_cstm on id = id_c
               left join users on users.id = assigned_user_id
      where date(jc_meetings_domru.date_entered) >= '2023-06-01'
        and (jc_meetings_domru.status <> 'Error' and jc_meetings_domru.status <> 'doubled' and
             jc_meetings_domru.status <> 'change_flat')#Domru
      union all
      select jc_meetings_netbynet.assigned_user_id   userid,
             case
                 when jc_meetings_netbynet.status in ('Held', 'Active') then 1
                 when jc_meetings_netbynet.status in
                      ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                 else 0
                 end                                 statused,
             date(jc_meetings_netbynet.date_entered) dateentered,
             if(length(replace(replace(replace(replace(jc_meetings_netbynet.phone_work, '-', ''), ')', ''), '(', ''),
                               ' ',
                               '')) <=
                10,
                concat(8,
                       replace(replace(replace(replace(jc_meetings_netbynet.phone_work, '-', ''), ')', ''), '(', ''),
                               ' ', '')),
                concat(8,
                       right(replace(replace(replace(replace(jc_meetings_netbynet.phone_work, '-', ''), ')', ''), '(',
                                             ''), ' ',
                                     ''), 10))) as   contact,
             jc_meetings_netbynet_cstm.last_queue_c  queue
      from jc_meetings_netbynet
               left join jc_meetings_netbynet_cstm on id = id_c
               left join users on users.id = assigned_user_id
      where date(jc_meetings_netbynet.date_entered) >= '2023-06-01'
        and (jc_meetings_netbynet.status <> 'Error' and jc_meetings_netbynet.status <> 'doubled' and
             jc_meetings_netbynet.status <> 'change_flat')#NBN

     ) req

