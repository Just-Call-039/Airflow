select datetime,
       date_stop,
       queue_num         mother_queue,
       queuenum_custom_c ro_queue,
       Campaign as campaign,
       hashtag_c         base_hashtag,
       name name_autofilling,
       marker,
       status,
       base_value_c base_count,
       sum(data.limit)         limits,
       sum(inserted)     facts
from (select DATE_ADD(
                     DATE_FORMAT(datetime, "%Y-%m-%d %H:00:00"),
                     INTERVAL IF(MINUTE(datetime) < 59, 0, 1) HOUR
                 )               datetime,
             jc_contactspusher_cstm.date_stop_c date_stop,
             adial_campaign.queue_num,
             adial_campaign_cstm.queuenum_custom_c,
             adial_campaign.name campaign,
             jc_contactspusher_cstm.hashtag_c,
             contacts_pusher_log.inserted,
             jc_contactspusher.name,
             jc_contactspusher.marker,
             jc_contactspusher_cstm.base_value_c,
             jc_contactspusher.status,
             contacts_pusher_log.limit
      from suitecrm.contacts_pusher_log
               left join suitecrm.jc_contactspusher on id = pusher_id
               left join suitecrm.jc_contactspusher_cstm on id_c = pusher_id
               left join suitecrm.jc_contactspusher_audit on jc_contactspusher_audit.id = pusher_id
               left join suitecrm.adial_campaign on campaign_id = adial_campaign.id
               left join suitecrm.adial_campaign_cstm on adial_campaign_cstm.id_c = campaign_id

     ) data

where date(date_stop) >= now() - interval 2 month
group by 1, 2, 3, 4, 5, 6, 7, 8, 9