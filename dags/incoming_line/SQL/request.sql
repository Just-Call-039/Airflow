select assigned_user_id,
       date_entered,
       phone_work,
       statused,
       last_queue_c,
       date_created,
       before_value_string,
       after_value_string
from (select jc_meetings_beeline.assigned_user_id   assigned_user_id,
             case
                 when jc_meetings_beeline.status in ('Held', 'Active') then 1
                 when jc_meetings_beeline.status in
                      ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                 else 0
                 end                                statused,
             date(jc_meetings_beeline.date_entered) date_entered,
             jc_meetings_beeline.phone_work         phone_work,
             jc_meetings_beeline_cstm.last_queue_c  last_queue_c,
             date_created,
             before_value_string,
             after_value_string
      from jc_meetings_beeline
               left join jc_meetings_beeline_cstm on id = id_c
               left join users on users.id = assigned_user_id
               left join jc_meetings_beeline_audit on jc_meetings_beeline.id = parent_id
      where date(jc_meetings_beeline.date_entered) >= '2023-05-01'
        and (jc_meetings_beeline.status <> 'Error' and jc_meetings_beeline.status <> 'doubled' and
             jc_meetings_beeline.status <> 'change_flat')
        and field_name = 'status'#beeline
      union all
      select jc_meetings_mts.assigned_user_id   assigned_user_id,
             case
                 when jc_meetings_mts.status in ('Held', 'Active') then 1
                 when jc_meetings_mts.status in
                      ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                 else 0
                 end                            statused,
             date(jc_meetings_mts.date_entered) date_entered,
             jc_meetings_mts.phone_work         phone_work,
             jc_meetings_mts_cstm.last_queue_c  last_queue_c,
             date_created,
             before_value_string,
             after_value_string
      from jc_meetings_mts
               left join jc_meetings_mts_cstm on id = id_c
               left join users on users.id = assigned_user_id
               left join jc_meetings_mts_audit on jc_meetings_mts.id = parent_id
      where date(jc_meetings_mts.date_entered) >= '2023-05-01'
        and (jc_meetings_mts.status <> 'Error' and jc_meetings_mts.status <> 'doubled' and
             jc_meetings_mts.status <> 'change_flat')
        and field_name = 'status'#MTS
      union all
      select jc_meetings_rostelecom.assigned_user_id   assigned_user_id,
             case
                 when jc_meetings_rostelecom.status in ('Held', 'Active') then 1
                 when jc_meetings_rostelecom.status in
                      ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                 else 0
                 end                                   statused,
             date(jc_meetings_rostelecom.date_entered) date_entered,
             jc_meetings_rostelecom.phone_work         phone_work,
             jc_meetings_rostelecom_cstm.last_queue_c  last_queue_c,
             date_created,
             before_value_string,
             after_value_string
      from jc_meetings_rostelecom
               left join jc_meetings_rostelecom_cstm on id = id_c
               left join users on users.id = assigned_user_id
               left join jc_meetings_rostelecom_audit on jc_meetings_rostelecom.id = parent_id
      where date(jc_meetings_rostelecom.date_entered) >= '2023-05-01'
        and (jc_meetings_rostelecom.status <> 'Error' and jc_meetings_rostelecom.status <> 'doubled' and
             jc_meetings_rostelecom.status <> 'change_flat')
        and field_name = 'status'#RTK
      union all
      select jc_meetings_ttk.assigned_user_id   assigned_user_id,
             case
                 when jc_meetings_ttk.status in ('Held', 'Active') then 1
                 when jc_meetings_ttk.status in
                      ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                 else 0
                 end                            statused,
             date(jc_meetings_ttk.date_entered) date_entered,
             jc_meetings_ttk.phone_work         phone_work,
             jc_meetings_ttk_cstm.last_queue_c  last_queue_c,
             date_created,
             before_value_string,
             after_value_string
      from jc_meetings_ttk
               left join jc_meetings_ttk_cstm on id = id_c
               left join users on users.id = assigned_user_id
               left join jc_meetings_ttk_audit on jc_meetings_ttk.id = parent_id
      where date(jc_meetings_ttk.date_entered) >= '2023-05-01'
        and (jc_meetings_ttk.status <> 'Error' and jc_meetings_ttk.status <> 'doubled' and
             jc_meetings_ttk.status <> 'change_flat')
        and field_name = 'status'#TTK
      union all
      select jc_meetings_domru.assigned_user_id   assigned_user_id,
             case
                 when jc_meetings_domru.status in ('Held', 'Active') then 1
                 when jc_meetings_domru.status in
                      ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                 else 0
                 end                              statused,
             date(jc_meetings_domru.date_entered) date_entered,
             jc_meetings_domru.phone_work         phone_work,
             jc_meetings_domru.last_queue_c       last_queue_c,
             date_created,
             before_value_string,
             after_value_string
      from jc_meetings_domru
               left join jc_meetings_domru_cstm on id = id_c
               left join users on users.id = assigned_user_id
               left join jc_meetings_domru_audit on jc_meetings_domru.id = parent_id
      where date(jc_meetings_domru.date_entered) >= '2023-05-01'
        and (jc_meetings_domru.status <> 'Error' and jc_meetings_domru.status <> 'doubled' and
             jc_meetings_domru.status <> 'change_flat')
        and field_name = 'status' #Domru
      union all
      select jc_meetings_netbynet.assigned_user_id   assigned_user_id,
             case
                 when jc_meetings_netbynet.status in ('Held', 'Active') then 1
                 when jc_meetings_netbynet.status in
                      ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                 else 0
                 end                                 statused,
             date(jc_meetings_netbynet.date_entered) date_entered,
             jc_meetings_netbynet.phone_work         phone_work,
             jc_meetings_netbynet_cstm.last_queue_c  last_queue_c,
             date_created,
             before_value_string,
             after_value_string
      from jc_meetings_netbynet
               left join jc_meetings_netbynet_cstm on id = id_c
               left join users on users.id = assigned_user_id
               left join jc_meetings_netbynet_audit on jc_meetings_netbynet.id = parent_id
      where date(jc_meetings_netbynet.date_entered) >= '2023-05-01'
        and (jc_meetings_netbynet.status <> 'Error' and jc_meetings_netbynet.status <> 'doubled' and
             jc_meetings_netbynet.status <> 'change_flat')
        and field_name = 'status'#NBN

     ) req

