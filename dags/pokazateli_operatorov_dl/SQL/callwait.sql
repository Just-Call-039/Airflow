-- Запрос на выгрузку информации по запланированным звонкам
-- call_date дата звонка, когда создан перезвон
-- date_start дата, на которую создана заявка на перезвон

SELECT assigned_user_id AS user_id,
       DATE(date_entered) AS call_date,
       contacts_status AS call_status,
       DATE(date_start) AS date_start,
       contact_id_c AS phone
  FROM suitecrm.jc_planned_calls 
   