def contact_editer(path_to_files, requests):
    import pandas as pd
    import glob
    import os
    from clickhouse_driver import Client

    requests = pd.read_csv(f'{path_to_files}/{requests}')

    print('Подключаемся к серверу')
    dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
    if dest:
        with open(dest) as file:
            for now in file:
                now = now.strip().split('=')
                first, second = now[0].strip(), now[1].strip()
                if first == 'host':
                    host = second
                elif first == 'user':
                    user = second
                elif first == 'password':
                    password = second
        # return host, user, password

 
    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})
    
    print('Удаляем таблицу req_x')
    client.execute('drop table suitecrm_robot_ch.req_x')

    print('Создаем таблицу req_x')
    sql_create_req = '''CREATE TABLE suitecrm_robot_ch.req_x
                    (
                        project       String,
                        phone_request String,
                        request_date  Date,
                        user          String,
                        super         String,
                        status        String
                    ) ENGINE = MergeTree()
                        order by phone_request'''
    client.execute(sql_create_req)
    
    print('Отправляем запрос')
    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.req_x VALUES',
                            requests[['project', 'phone_request', 'request_date', 'user', 'super', 'status']])


    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})
    
    print('Удаляем таблицу category_x')
    client.execute('DROP table suitecrm_robot_ch.category_x')

    print('Создаем таблицу category_x')
    sql_create_category = '''CREATE TABLE suitecrm_robot_ch.category_x
                    (
                        category Int32,
                        phone    String
                    ) ENGINE = MergeTree()
                        order by category'''
    client.execute(sql_create_category)
    print('Добавляем каждому номеру телефону категорию')
    sql_insert_category = '''insert into suitecrm_robot_ch.category_x
                    select count(phone) as category, phone
                    from suitecrm_robot_ch.jc_robot_log
                    where toDate(call_date) >= '2022-04-01'
                        and toDate(call_date) < today()
                        and last_step not in ('', '0', '111', '371', '372', '362', '361', '261', '262')
                    group by phone'''
    
    client.execute(sql_insert_category)


    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})
    
    print('Удаляем таблицу req_id')
    client.execute('drop table suitecrm_robot_ch.req_id')

    print('Создаем таблицу req_id')
    sql_create_reqid = '''CREATE TABLE suitecrm_robot_ch.req_id
                    (
                        project       String,
                        phone_request String,
                        request_date  Date,
                        user          String,
                        super         String,
                        status        String,,
                        call_date     Nullable(DateTime),
                        last_step     Nullable(String),
                        uniqueid      Nullable(String)
                    ) ENGINE = MergeTree()
                        order by phone_request'''
    client.execute(sql_create_reqid)
    print('Добавляем каждой заявке уникальный айди')
    sql_insert_reqid = '''insert into suitecrm_robot_ch.req_id
                select distinct project,
                phone_request,
                request_date,
                user,
                super,
                status,
                call_date,
                last_step,
                    CASE
                WHEN request_date >= toDate(call_date) AND request_date <= addDays(toDate(call_date), 30)
              THEN uniqueid
            ELSE NULL end as unique_id
                from suitecrm_robot_ch.req_x as req
                          left join (select phone, call_date, uniqueid, last_step
                    from suitecrm_robot_ch.jc_robot_log
                         --left join suitecrm_robot_ch.calls_log on calls_log.phone = jc_robot_log.phone and
                         --                                      toDate(jc_robot_log.call_date) =
                         --                                    toDate(calls_log.call_date)
                    where toDate(call_date) >= '2022-04-01'
                      and toDate(call_date) < today()
                      and
                      --last_step not in ('', '0', '2', '111', '371', '372', '362', '361', '261', '262')
                            last_step in
                            ('7', '39', '41', '49', '51', '53', '55', '56', '57', '59', '61', '63', '65', '67', '69',
                             '71', '72', '73', '75', '76', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88',
                             '89', '91', '92', '93', '94', '95', '96', '97', '98', '99', '119', '120', '122', '355',
                             '381', '386', '399', '500')
    ) as jc on jc.phone = req.phone_request'''
    
    client.execute(sql_insert_reqid)

    print('Загружаем уже полностью скрипт в основную таблицу с номерами телефона')
    sql_insert_contact_of_req = '''insert into suitecrm_robot_ch.contact_of_req
    with jc_rl as (select toDate(call_date) as call_date,
                      phone,
                      case
                          when ptv_c like '%^3^%'
                              or ptv_c like '%^5^%'
                              or ptv_c like '%^6^%'
                              or ptv_c like '%^10^%'
                              or ptv_c like '%^11^%'
                              or ptv_c like '%^19^%'
                              or ptv_c like '%^14^%' then 'Разметка Наша'
                          when ptv_c like '%^3_19^%'
                              or ptv_c like '%^5_19^%'
                              or ptv_c like '%^6_19^%'
                              or ptv_c like '%^10_19^%'
                              or ptv_c like '%^11_19^%'
                              or ptv_c like '%^19_19^%'
                              or ptv_c like '%^14_19^%' then 'Разметка не наша 50+'
                          when
                                      ptv_c like '%^3_21^%'
                                  or ptv_c like '%^5_21^%'
                                  or ptv_c like '%^6_21^%'
                                  or ptv_c like '%^10_21^%'
                                  or ptv_c like '%^11_21^%'
                                  or ptv_c like '%^19_21^%'
                                  or ptv_c like '%^14_21^%' then 'Разметка не наша Телеком'
                          when
                                      ptv_c like '%^3_18^%'
                                  or ptv_c like '%^5_18^%'
                                  or ptv_c like '%^6_18^%'
                                  or ptv_c like '%^10_18^%'
                                  or ptv_c like '%^11_18^%'
                                  or ptv_c like '%^19_18^%'
                                  or ptv_c like '%^14_18^%' then 'Разметка не наша 50-40'
                          when
                                      ptv_c like '%^5_20^%'
                                  or ptv_c like '%^3_20^%'
                                  or ptv_c like '%^6_20^%'
                                  or ptv_c like '%^10_20^%'
                                  or ptv_c like '%^11_20^%'
                                  or ptv_c like '%^19_20^%'
                                  or ptv_c like '%^14_20^%' then 'Разметка не наша Спутник'
                          when
                                      ptv_c like '%^3_17^%'
                                  or ptv_c like '%^5_17^%'
                                  or ptv_c like '%^6_17^%'
                                  or ptv_c like '%^10_17^%'
                                  or ptv_c like '%^11_17^%'
                                  or ptv_c like '%^19_17^%'
                                  or ptv_c like '%^14_17^%' then 'Разметка не наша 40-30'
                          when
                                      ptv_c like '%^5_16^%'
                                  or ptv_c like '%^3_16^%'
                                  or ptv_c like '%^6_16^%'
                                  or ptv_c like '%^10_16^%'
                                  or ptv_c like '%^11_16^%'
                                  or ptv_c like '%^19_16^%'
                                  or ptv_c like '%^14_16^%' then 'Разметка не наша 30-20'
                          when
                                      ptv_c like '%^5_15^%'
                                  or ptv_c like '%^3_15^%'
                                  or ptv_c like '%^6_15^%'
                                  or ptv_c like '%^10_15^%'
                                  or ptv_c like '%^11_15^%'
                                  or ptv_c like '%^19_15^%'
                                  or ptv_c like '%^14_15^%' then 'Разметка не наша 20-0'
                          else ''
                          end              ptv,
                      case
                          when region_c = 1 then 'Наша полная'
                          when region_c = 2 then 'Наша неполная'
                          when region_c = 4 then 'Фиас из разных источников'
                          when region_c = 5 then 'Фиас до города'
                          when region_c = 6 then 'Старый town_c'
                          when region_c = 7 then 'Def code'
                          else ''
                          end              region,
                      uniqueid
               from (select jc.phone,
                            region_c,
                            uniqueid,
                            last_step,
                            call_date,
                            if(o25.ptv_c is null, jc.ptv_c, o25.ptv_c) ptv_c
                     from (select *
                           from suitecrm_robot_ch.jc_robot_log
                           where toDate(call_date) = yesterday()
                              --and toDate(call_date) <= '2023-09-05'
                              ) as jc
                              left join (select *
                                         from suitecrm_robot_ch.otchet_25
                                         where toDate(my_date) = yesterday()
                         --and toDate(my_date) <= '2023-09-05'
                         ) as o25
                                        on o25.phone = jc.phone and toDate(call_date) = toDate(my_date)
                     where last_step not in ('', '0', '111', '371', '372', '362', '361', '261', '262')
                        )),

     requests as (select toDate(requests.request_date) as request_date, requests.phone_request, uniqueid
                  from suitecrm_robot_ch.req_id as requests
                  where toDate(request_date) >= '2022-04-01'),
     tab1 as (select jc_rl.call_date                                     dates,
                     jc_rl.phone                                         phones,
                     category_x.category                                 category,
                     jc_rl.ptv                                           ptv,
                     jc_rl.region                                        region,
                     request_date,
                     if(phone_request in ('', ' '), null, phone_request) phone_request
              from jc_rl
                       left join suitecrm_robot_ch.category_x on jc_rl.phone = category_x.phone
                       left join requests on jc_rl.uniqueid = requests.uniqueid)

    select dates,
           phones,
           category,
           ptv,
           region,
           if(phone_request is null, null, request_date) request_date,
           phone_request
    from tab1'''
    
    client.execute(sql_insert_contact_of_req)

    print('Добавление данных полностью завершилось')
    print('Добавление данных полностью завершилось')
    print('Добавление данных полностью завершилось')
