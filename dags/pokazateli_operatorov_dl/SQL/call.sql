-- Выгрузка данных из журнала звонков за конкретный день
-- Джойним с calls_cstm, чтобы достать номер телефона и очередь оператора
-- Джойним с contacts и contacts_cstm, чтобы достать города
-- Джойним с jc_robot_log (старым и новым), чтобы достать очередь набирающую
-- date_i - переменная с датой (тип строка), за которую требуется выборка


SELECT 
       DATE(cl.date_entered) as call_date,
       clc.asterisk_caller_id_c as phone,
       clc.queue_c as queue_c,   
       r.dialog as dialog,
       cl.assigned_user_id as user_id,
     --   cl_c.user_id_c as supervisor,
       CASE WHEN cl.name = '** Авто-запись **' THEN 'auto'
            WHEN cl.name = 'Входящий звонок' THEN 'inbound'
            WHEN cl.name = 'Исходящий звонок' THEN 'outbound' 
            END  as name,
       ccc.city_c as city,
       ccc.town_c as town,
       cl.duration_minutes as call_sec,
       IF(cl.duration_minutes <= 10, 1, 0) as short_call
  FROM suitecrm.calls cl
       LEFT JOIN suitecrm.calls_cstm clc ON cl.id = clc.id_c
       LEFT JOIN suitecrm.contacts cc ON clc.asterisk_caller_id_c = cc.phone_work
       LEFT JOIN suitecrm.contacts_cstm ccc ON ccc.id_c = cc.id
       LEFT JOIN (
                  SELECT phone,
                         REGEXP_SUBSTR(dialog, '[0-9]+') as dialog,
                         call_date
                    FROM suitecrm_robot.jc_robot_log
                   WHERE date(call_date) = {date_i}

                   UNION ALL 

                  SELECT phone,
                         robot_id as dialog,
                         call_date
                    FROM suitecrm_robot.robot_log 
                         LEFT JOIN suitecrm_robot.robot_log_addition 
                                ON robot_log.id = robot_log_addition.robot_log_id
                   WHERE date(call_date) = {date_i}
               ) r ON r.phone = clc.asterisk_caller_id_c
 WHERE DATE(cl.date_entered) = {date_i}
