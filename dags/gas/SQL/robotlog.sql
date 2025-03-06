-- Выгрузка входящих звонков 
-- date_i - дата, за которую выгружаем
-- phones - список телефонов из базы газа

select phone, 
       date(call_date) call_date, 
       last_step, 
       REGEXP_SUBSTR(dialog, '[0-9]+') queue, 
       if(route like '262%', 1, 0) inbound
  from suitecrm_robot.jc_robot_log
 where date(call_date) = date(now())
     --   and phone in ({phones})