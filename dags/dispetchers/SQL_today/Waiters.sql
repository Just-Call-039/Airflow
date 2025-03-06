SELECT 
       CASE
            WHEN LENGTH(caller_id) >= 12 THEN RIGHT(caller_Id, 11)
       ELSE caller_id
       END              caller_id,
       queue_num_curr
  FROM waiter_log
 WHERE (caller_id REGEXP '^[0-9]*$')
       and hangup_time IS NOT NULL
       and LENGTH(caller_id) >= 11
       and caller_id not in (
           SELECT asterisk_caller_id_c
                  FROM suitecrm.calls_cstm cc
                       LEFT JOIN suitecrm.calls
                            ON id_c = id

                WHERE date(date_entered) = date(now())
                    )