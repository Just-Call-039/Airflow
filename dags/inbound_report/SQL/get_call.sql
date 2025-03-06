SELECT 
                       if(cc.asterisk_caller_id_c = 'anonymous', '80000000000', cc.asterisk_caller_id_c)   phone,
                       (c.date_entered + interval 3 hour)           date,
                       c.assigned_user_id                       userid,
                       cc.result_call_c                             result_call,
                       cc.otkaz_c                                   otkaz_c,
                       if(cc.queue_c is null, '0', cc.queue_c)      queue_c
                  FROM suitecrm.calls c
                       LEFT JOIN suitecrm.calls_cstm cc
                            ON id_c = id
                            LEFT JOIN suitecrm.users 
                                ON assigned_user_id = users.id
                        
                 WHERE date(c.date_entered) BETWEEN '{}' AND '{}'
                       and name in ('Входящий звонок', 'Автодозвон')