def transfer_to_click(x, y, stop, general_create):
    import pandas as pd
    import pymysql
    from clickhouse_driver import Client
    from datetime import datetime
    import math

    print('Подключаемся к clickhouse')
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

    try:
        print('Удаляем старую таблицу')
        client = Client(host=host, port='9000', user=user, password=password,
                        database='suitecrm_robot_ch', settings={'use_numpy': True})
        
        sql_drop = '''truncate table suitecrm_robot_ch.contacts_cstm'''
        client.execute(sql_drop)

    # sql_drop = '''drop table suitecrm_robot_ch.contacts_cstm'''
    # client.execute(sql_drop)

    # print('Создаем новую таблицу')
    # sql_create = open(general_create).read().replace('п»ї','').replace('﻿','').replace('\ufeff','')
    # client.execute(sql_create)
    except (ValueError):
        print('Данные не удалены')
    finally:

        client.connection.disconnect()
        print('conection closed')
    try:

        sql_check_volume = ''' select count(*) as contacts
                            from gar.contacts '''

        client = Client(host=host, port='9000', user=user, password=password,
                        database='suitecrm_robot_ch', settings={'use_numpy': True})
        
        # check_volume = pd.read_sql_query(sql_check_volume, Con)
        check_volume = pd.DataFrame(client.query_dataframe(sql_check_volume))
    except (ValueError):
        print('Данные не выгружены')

    finally:

        client.connection.disconnect()
        print('conection closed')
       
    rounds = math.ceil(check_volume.contacts[0]/y)
    print(f'Всего строк {check_volume.contacts[0]}')
    print(f'Всего кругов {rounds}')
    n = 0
        
    for i in range(0,rounds):    
        print(f'Выгрузка _______________ {i}')
        print(datetime.today().strftime("%m/%d/%Y, %H:%M:%S"))

        try:

            sql = f'''select id,phone_work,last_call_c,'' as priority1,'' as priority2,ptv_c, 0 as next_project, 0 as last_project,stoplist_c,base_source_c,town_c,city_c,marker_c,step_c,last_queue_c,region_c,network_provider_c,otkaz_c,contacts_status_c
            from gar.contacts
            LEFT JOIN gar.contacts_cstm ON contacts.id = contacts_cstm.id_c
            where ptv_c like '%^3^%'
            or ptv_c like '%^5^%'
            or ptv_c like '%^6^%'
            or ptv_c like '%^10^%'
            or ptv_c like '%^11^%'
            or ptv_c like '%^19^%'
            limit {x},{y}'''

            client = Client(host=host, port='9000', user=user, password=password,
                        database='suitecrm_robot_ch', settings={'use_numpy': True})
            
            contacts = pd.DataFrame(client.query_dataframe(sql))
            print(f'Размер {contacts.shape[0]}')
        except (ValueError):
            print('Данные не выгружены')

        finally:

            client.connection.disconnect()
            print('conection closed')

        

        print('Правим')
        contacts[['next_project', 'last_project','last_queue_c','marker_c']] = contacts[['next_project', 'last_project','last_queue_c','marker_c']].fillna(0).astype('int')
        contacts[['id', 'phone_work', 'priority1', 'priority2', 'ptv_c',
            'next_project', 'last_project', 'stoplist_c', 'base_source_c', 'town_c',
            'city_c', 'marker_c', 'step_c', 'last_queue_c', 'region_c',
            'network_provider_c', 'otkaz_c', 'contacts_status_c']] = contacts[['id', 'phone_work', 'priority1', 'priority2', 'ptv_c',
            'next_project', 'last_project', 'stoplist_c', 'base_source_c', 'town_c',
            'city_c', 'marker_c', 'step_c', 'last_queue_c', 'region_c',
            'network_provider_c', 'otkaz_c', 'contacts_status_c']].fillna('0').astype('str')
        contacts['last_call_c'] = pd.to_datetime(contacts['last_call_c'], errors="coerce").fillna('')

        print('Загружаем данные')

        try:

            client = Client(host=host, port='9000', user=user, password=password,
                        database='suitecrm_robot_ch', settings={'use_numpy': True})

            client.insert_dataframe('INSERT INTO suitecrm_robot_ch.contacts_cstm VALUES', contacts)
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

