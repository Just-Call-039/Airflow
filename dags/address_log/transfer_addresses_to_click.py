def transfer_to_click(x, y, stop, general_create):
    import pandas as pd
    import pymysql
    from clickhouse_driver import Client
    from datetime import datetime
    import math

    print('Подключаемся к clickhouse')
    dest = '/root/airflow/dags/not_share/ClickHouse198.csv'
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

    

    # print('Удаляем старую таблицу')
    # client = Client(host=host, port='9000', user=user, password=password,
    #                 database='suitecrm_robot_ch', settings={'use_numpy': True})

    # sql_drop = '''drop table suitecrm_robot_ch.address_log'''
    # client.execute(sql_drop)

    # print('Создаем новую таблицу')
    # sql_create = open(general_create).read().replace('п»ї','').replace('﻿','').replace('\ufeff','')
    # client.execute(sql_create)


    print('Подключаемся к mysql')
    dest = '/root/airflow/dags/not_share/cloud_my_sql_128.csv'
    if dest:
        with open(dest) as file:
            for now in file:
                now = now.strip().split('=')
                first, second = now[0].strip(), now[1].strip()
                if first == 'host':
                    host2 = second
                elif first == 'user':
                    user2 = second
                elif first == 'password':
                    password2 = second

    sql_check_volume = '''select count(*) as contacts
                        from ( select * from suitecrm.address_log
                        where date(calldate) = date(now())
                        #   - interval 1 day
                        # limit 1000
                        ) tt '''


    Con = pymysql.Connect(host=host2, user=user2, passwd=password2, db="suitecrm",
                        charset='utf8')
    check_volume = pd.read_sql_query(sql_check_volume, Con)
    
    rounds = math.ceil(check_volume.contacts[0]/y)
    print(f'Всего строк {check_volume.contacts[0]}')
    print(f'Всего кругов {rounds}')
    n = 0

        
    for i in range(0,rounds):    
        print(f'Выгрузка _______________ {i}')
        print(datetime.today().strftime("%m/%d/%Y, %H:%M:%S"))

        sql = f'''select uniqueid  ,
                        calldate  ,
                        phone     ,
                        dialog    ,
                        city      ,
                        street    ,
                        house     ,
                        korp      ,
                        providers 
                    from suitecrm.address_log
                    where date(calldate) = date(now()) 
                    #  - interval 1 day
        limit {x},{y}'''

        Con = pymysql.Connect(host=host2, user=user2, passwd=password2, db="suitecrm",
                        charset='utf8')
        contacts = pd.read_sql_query(sql, Con)
        print(f'Размер {contacts.shape[0]}')

        

        print('Правим')
        contacts[['uniqueid','phone','dialog','city','street','house','korp','providers']] = contacts[['uniqueid','phone','dialog','city','street','house','korp','providers']].fillna('0').astype('str')
        contacts['calldate'] = pd.to_datetime(contacts['calldate'], errors="coerce").fillna('')
        print(contacts['calldate'].unique())

        print('Загружаем данные')

        try:

            client = Client(host=host, user=user, password=password,
                        database='suitecrm_robot_ch', settings={'use_numpy': True})

            client.insert_dataframe('INSERT INTO suitecrm_robot_ch.address_log VALUES', contacts)
        except (ValueError):
            print('Данные не загружены')
        finally:

            client.connection.disconnect()
            print('conection closed')
        
        x += y
        n += 1

        if n == stop:
            if stop > 0:
                print('STOP')
                break

