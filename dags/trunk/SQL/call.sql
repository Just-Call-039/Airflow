select c.date_entered + interval 3 hour,
                       cc.asterisk_caller_id_c,
                       if (cc.otkaz_c = 'otkaz_23', 1, 0) as avtootvetchik
                  from suitecrm.calls_cstm as cc
                       left join suitecrm.calls as c
                            on c.id = cc.id_c
                 where date(date_entered) = date(now())