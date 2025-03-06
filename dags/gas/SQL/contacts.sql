-- Запрос на выгрузку данных по контактам, фильтруем по очереди 9110 (договорились, 
-- что так помечаются контакты, залитые нам газом)


select phone_work as phone, 
                    if(date(last_call_c) = now(), date(last_call_c), '1970-01-01') call_date, 
                    last_queue_c as last_queue, 
                    step_c as last_step, 
                    contacts_status_c as contact_status, 
                    adial_campaign.name as campaign_name
                    
  from suitecrm.adial_campaign_contacts_c 
       left join suitecrm.contacts on contacts.id = adial_campaign_contactscontacts_idb
       left join suitecrm.contacts_cstm  on id_c = adial_campaign_contactscontacts_idb
       left join suitecrm.adial_campaign on adial_campaign.id = adial_campaign_contactsadial_campaign_ida
 where queue_num = 9110