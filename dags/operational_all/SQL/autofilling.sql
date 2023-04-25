select datetime,
       queue_num         Mother,
       queuenum_custom_c RO,
       Campaign,
       hashtag_c         Data,
       name,
       marker,
       sum(data.limit)         Limits,
       sum(inserted)     Facts
from (select DATE_ADD(
                     DATE_FORMAT(datetime, "%Y-%m-%d %H:00:00"),
                     INTERVAL IF(MINUTE(datetime) < 59, 0, 1) HOUR
                 )               datetime,
             date(datetime)      date,
             pusher_id,
             queue_num,
             queuenum_custom_c,
             adial_campaign.name campaign,
             hashtag_c,
             contacts_pusher_log.need,
             contacts_pusher_log.inserted,
             contacts_pusher_log.result,
             jc_contactspusher.name,
             marker,
             campaign1,
             campaign1_percent,
             count,
             status,
             contacts_pusher_log.limit
      from suitecrm.contacts_pusher_log
               left join suitecrm.jc_contactspusher on id = pusher_id
               left join suitecrm.jc_contactspusher_cstm on id_c = pusher_id
               left join suitecrm.adial_campaign on campaign_id = adial_campaign.id
               left join suitecrm.adial_campaign_cstm on adial_campaign_cstm.id_c = campaign_id
      where
            (date(datetime) = date(now()) or (date(datetime) = date(now()) - 1 and time(datetime) >= '18:00:00'))
#     datetime between '2022-07-19 18:00:00' and '2022-07-27 18:00:00'
     ) data
group by 1, 2, 3, 4, 5, 6, 7
