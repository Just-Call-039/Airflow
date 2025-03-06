def operational_calls_transformation(path_to_folder, name_calls, path_to_final_folder):
    import pandas as pd
    import operational_all.redactor_operational_calls as redactor
    from clickhouse_driver import Client
    import datetime
    from commons_liza import to_click

    df = pd.read_csv(f'{path_to_folder}/{name_calls}')
    print('Изменение типа колонки')
#     print(df.columns)
    df['talk'] = df['talk'].fillna(0).astype('int')
    df['perevod'] = df['perevod'].fillna(0).astype('int')
    df['meeting'] = df['meeting'].fillna(0).astype('int')

    df_phone = df[['phone_work']].astype('str').drop_duplicates()
    
    # dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
    # if dest:
    #     with open(dest) as file:
    #         for now in file:
    #             now = now.strip().split('=')
    #             first, second = now[0].strip(), now[1].strip()
    #             if first == 'host':
    #                 host = second
    #             elif first == 'user':
    #                 user = second
    #             elif first == 'password':
    #                 password = second
    try:
        print('Подключаемся к серверу')
        # client = Client(host=host, port='9000', user=user, password=password,
        #             database='suitecrm_robot_ch', settings={'use_numpy': True})

        client = to_click.my_connection_174()
        
        
        print('Проверка застрявшей таблицы')

        if pd.DataFrame(client.execute('''show tables from suitecrm_robot_ch where table = 'temp_operational2'  ''')).shape[0] == 0:
            print('Таблицы нет') 
        else:
            print('Удаляю застрявшую')
           
            client.execute('''drop table suitecrm_robot_ch.temp_operational2 ''')



        print('Создаем таблицу')
        sql_create = '''create table suitecrm_robot_ch.temp_operational2
                        (
                            phone_work String
                        ) ENGINE = MergeTree
                            order by phone_work'''
        
        client.execute(sql_create)

    except (ValueError):
        print('Данные не загружены, таблица не удалена')
    finally:

        client.connection.disconnect()
        print('conection closed')
    try:
        # client = Client(host=host, port='9000', user=user, password=password,
        #                 database='suitecrm_robot_ch', settings={'use_numpy': True})
        client = to_click.my_connection_174()

        print('Отправляем запрос и получаем категории')
        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.temp_operational2 VALUES', df_phone)



    
        click_sql = '''select temp_operational2.phone_work as phone_work, case when calls between 1 and 5 then '1-5 calls'
                        when calls between 6 and 10 then '6-10 calls'
                            when calls between 11 and 15 then '11-15 calls'
                                when calls > 15 then '> 15 calls'
                                    else '0' end category_calls,
                    case when answer between 1 and 5 then '1-5 answers'
                        when answer between 6 and 10 then '6-10 answers'
                                when answer > 10 then '> 10 answers'
                                    else '0' end category
                from suitecrm_robot_ch.temp_operational2
                        left join (select phone, count(date) calls
                                    from (
                                            select distinct concat('8',substring(dst, 2, 10)) phone,
                                                            toDate(calldate)      date
                                            from asteriskcdrdb_all.cdr_all
                                            where toDate(calldate) between (toDate(now()) - interval 357 day) and (toDate(now()) - interval 1 day)
                                            and concat('8',substring(dst, 2, 10)) in (select *
                                                                            from suitecrm_robot_ch.temp_operational2)
                                            ) tt
                                    group by phone) cdr on temp_operational2.phone_work = cdr.phone
                        left join (select phone, count(dialog) answer
                                    from (
                                            select phone,
                                                    dialog
                                            from suitecrm_robot_ch.jc_robot_log
                                            where toDate(call_date) between (toDate(now()) - interval 357 day) and (toDate(now()) - interval 1 day)
                                            and last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
                                            and phone in (select *
                                                                            from suitecrm_robot_ch.temp_operational2)
                                            ) t
                                    group by phone) jc on temp_operational2.phone_work = jc.phone'''
        
        category = pd.DataFrame(client.query_dataframe(click_sql))

        print('Удаляем таблицу')
        client.execute('drop table suitecrm_robot_ch.temp_operational2')
    except (ValueError):
        print('Данные не загружены, таблица не удалена')
    finally:

        client.connection.disconnect()
        print('conection closed')

    df['phone_work'] = df['phone_work'].astype('str').apply(lambda x: x.replace('.0',''))
    category['phone_work'] = category['phone_work'].astype('str').apply(lambda x: x.replace('.0',''))
    df = df.merge(category, how='left', on='phone_work')
    print(df.columns)
    print(df['network_provider'].unique())

    df['network_provider'] = df.apply(lambda row: redactor.network(row), axis=1)

    print(df.columns)

    df = df.groupby(['project',
        'calldate',
        # 'callhour',
        'network_provider',
        'count_good_calls_c',
        'data',
        'last_queue_c',
        'custom_queue_c',
        'marker_c',
        'town_c',
        'city_c',
        'category_calls',
        'category_y',
        'stop_auto',
        'region_c2',
        'talk',
        'id_c',
        'perevod',
        'meeting'], as_index=False, dropna=False).agg({'talk': 'sum',
                                                                     'id_c': 'count',
                                                                     'perevod': 'sum',
                                                                     'meeting': 'sum'}).rename(columns={'talk': 'Разговоры',
                                                                     'id_c': 'Звонки',
                                                                     'perevod': 'Переводы',
                                                                     'meeting': 'Заявки',
                                                                     'data': 'База',
                                                                     'category_y': 'category'})

    print('Переименование колонок')
    df = df.rename(columns={'пїЅпїЅпїЅпїЅ': 'База',
            'пїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅ': 'Разговоры',
            'пїЅпїЅпїЅпїЅпїЅпїЅ': 'Звонки',
            'пїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅ': 'Переводы',
            'пїЅпїЅпїЅпїЅпїЅпїЅ.1': 'Заявки',
            'Р\xa0Р°Р·РіРѕРІРѕСЂС‹': 'Разговоры',
            'Р—РІРѕРЅРєРё': 'Звонки',
            'РџРµСЂРµРІРѕРґС‹': 'Переводы',
            'Р—Р°СЏРІРєРё': 'Заявки',
            'Р‘Р°Р·Р°': 'База'})

    # df['network_provider'] = df.apply(lambda row: redactor.network(row), axis=1)
    df['База'] = df.apply(lambda row: redactor.data(row), axis=1)

    print('Сохраняем')
    df.to_csv(f'{path_to_final_folder}/{name_calls}', sep=',', index=False, encoding='utf-8')

# operational_calls_transformation(path_to_folder = '/root/airflow/dags/operational_all/Files/sql_operational/',
#                                  name_calls = 'operational_calls.csv',
#                                  path_to_final_folder = '/root/airflow/dags/operational_all/Files/operational/'
#                                 )

    # Отправка в клик

    # print('Подключаемся к clickhouse')
    # dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
    # if dest:
    #     with open(dest) as file:
    #         for now in file:
    #             now = now.strip().split('=')
    #             first, second = now[0].strip(), now[1].strip()
    #             if first == 'host':
    #                 host = second
    #             elif first == 'user':
    #                 user = second
    #             elif first == 'password':
    #                 password = second
    #     # return host, user, password

    try:
        # client = Client(host=host, port='9000', user=user, password=password,
        #                 database='suitecrm_robot_ch', settings={'use_numpy': True})
        client = to_click.my_connection()


        # Формируем SQL-запрос для удаления строк
        cluster = '{cluster}'
        today = datetime.date.today()
        sql = f'''ALTER TABLE nakopitelny_nedozvons ON CLUSTER '{cluster}' DELETE WHERE calldate = toDate({today}) '''

        # Отправляем запрос
        client.execute(sql)
    except (ValueError):
        print('Таблица не удалена')
    finally:

        client.connection.disconnect()
        print('conection closed')

    column_list_str = ['count_good_calls_c', 'last_queue_c', 'custom_queue_c', 'marker_c', 'town_c', 'city_c', 'stop_auto']

    for column in column_list_str:
        df[column] = df[column].astype(str)
   
    
    df['calldate'] = df['calldate'].astype('datetime64').dt.date

   

    df = df.rename(columns={'База' : 'base',        
                    'Разговоры' : 'talks',    
                    'Звонки' : 'calls',    
                    'Переводы' : 'perevod',     
                    'Заявки' : 'meeting'})

    print('Подключаемся к clickhouse')

    # try:
    #     # client = Client(host=host, port='9000', user=user, password=password,
    #     #                 database='suitecrm_robot_ch', settings={'use_numpy': True})
    #     client = to_click.my_connection()
            
    #     print('Отправляем запрос')
    
    #     client.insert_dataframe('INSERT INTO suitecrm_robot_ch.nakopitelny_nedozvons VALUES', df)
    # except:
    #    raise
    # finally:

    #     client.connection.disconnect()
    #     print('conection closed')


    try:
        # client = Client(host='192.168.1.174', user=user, password=password,
        #                 database='suitecrm_robot_ch', settings={'use_numpy': True})

        client = to_click.my_connection()
            
        print('Отправляем запрос')
    
        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.nakopitelny_nedozvons VALUES', df)
        print('ok')
    except:
       raise
    finally:

        client.connection.disconnect()
        print('conection closed')


    