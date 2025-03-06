select

        phone_work,
        stoplist_c,
        ptv_c,
        if(ptv_c not like '%^n^%', 0, 1)                                                        ntv_ptv,
        step_c,
        region_c,
        network_provider_c,
        base_source_c,
        town_c,
        city_c,
        priority1,
        priority2,   
        date(last_call_c) last_call      
        
from suitecrm.contacts_cstm
left join suitecrm.contacts on id=id_c
left join suitecrm.contacts_custom_fields_new ON contacts_custom_fields_new.id_custom = contacts.id
where deleted=0
limit {}, {}
     