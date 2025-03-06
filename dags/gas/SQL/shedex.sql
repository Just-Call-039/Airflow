-- Запрос на загрузку данных из шедекса 
-- date_i - дата за которую нужна выгрузка

SELECT id_address, 
       aoc_name as territory_name, 
       vehicle_name as brigade, 
       status as request_status, 
       date(date) req_date, 
       mID as shedex_id
  FROM suitecrm.schedex
 WHERE date(date) = date(now())