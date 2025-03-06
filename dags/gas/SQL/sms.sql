-- Выгрузка информации об отправленных смс 
-- date_i - дата, за которую требуется выгрузить инфо

select 
       date(date_send) sms_date, 
       CONCAT('8', SUBSTRING(target_number, 2, 10)) as phone, 
       status as sms_status, 
       message_text

  from suitecrm.send_sms 
 where sender_number = 'GAZ39.RU'  
       and date(date_send) = date(now())