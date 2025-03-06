-- Запрос на вугрузку из бд gasification, джойним таблицы, чтобы достать инфо по бригаде, адресу, дате проверки и тд
-- phones - cписок телефонов из контактов, которые нам передал газ


select cleaned_phone as phone,  
                     contract, 
                     address, 
                     flat, 
                     brigade.name as brigade, 
                     territory_name, 
                     id_client_address, 
                     date plan_date, 
                     service
                     
                from gasification.client
                     left join gasification.client_address on id_client = client.id
                     left join gasification.address on address.id = id_address
                     left join gasification.brigade on id_brigade = brigade.id
                     left join gasification.client_service on client_service.id_client_address = client_address.id
                     left join gasification.service on service.id = id_service
               where client.cleaned_phone in ({phones}) 