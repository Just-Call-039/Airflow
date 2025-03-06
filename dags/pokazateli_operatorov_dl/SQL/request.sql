-- Скрипт для выгрузки заявок
-- period_request - переменная типа строка, в которой хранится дата для ограничения
--  выгрузки заявок (пока устанавливаем 4 месяца), чтобы не грузить весь архив

SELECT       
       phone_work as request_phone,
       date(date_entered) as request_date,       
       assigned_user_id as user_id,       
       status as request_status  

  FROM (SELECT              
               phone_work,              
               date_entered,              
               assigned_user_id,              
               status         
          FROM suitecrm.jc_meetings_rostelecom        
         WHERE status NOT IN ('Error', 'doubled', 'change_flat')              
               AND date(date_entered) > {period_request}

        UNION ALL   

       SELECT              
              phone_work,
              date_entered,              
              assigned_user_id,              
              status         
         FROM suitecrm.jc_meetings_beeline        
        WHERE status NOT IN ('Error', 'doubled', 'change_flat')              
              AND date(date_entered) > {period_request} 

        UNION ALL        

       SELECT              
              phone_work,              
              date_entered,              
              assigned_user_id,              
              status         
         FROM suitecrm.jc_meetings_netbynet        
        WHERE status NOT IN ('Error', 'doubled', 'change_flat')              
              AND date(date_entered) > {period_request}

        UNION ALL   

       SELECT              
              phone_work,             
              date_entered,              
              assigned_user_id,              
              status         
         FROM suitecrm.jc_meetings_mts        
        WHERE status NOT IN ('Error', 'doubled', 'change_flat')              
              AND date(date_entered) > {period_request}     

        UNION ALL  

       SELECT              
               phone_work,              
               date_entered,              
               assigned_user_id,              
               status         
          FROM suitecrm.jc_meetings_ttk       
         WHERE status NOT IN ('Error', 'doubled', 'change_flat')              
               AND date(date_entered) > {period_request}    

        UNION ALL   

       SELECT              
               phone_work,              
               date_entered,              
               assigned_user_id,              
               status         
          FROM suitecrm.jc_meetings_other        
         WHERE status NOT IN ('Error', 'doubled', 'change_flat')              
           AND date(date_entered) > {period_request}     

        UNION ALL   

       SELECT              
              phone_work,              
              date_entered,              
              assigned_user_id,              
              status         
         FROM suitecrm.jc_meetings_domru        
        WHERE status NOT IN ('Error', 'doubled', 'change_flat')              
              AND date(date_entered) > {period_request}

        ) as request
