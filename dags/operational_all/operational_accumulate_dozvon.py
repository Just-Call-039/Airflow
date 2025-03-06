# Функция для перемещения файла с сервера на сервер DBS.
# Необходимо передать абсолютный путь на обоих серверах, название файла, наименование сервера, куда перемещаем файл.
import time

def transfer_dozvon_to_dbs(from_path, file, to_path, file_result, db):

    from time import sleep
    from commons.connect_db import connect_db
    from smb.SMBConnection import SMBConnection

    host, user, password = connect_db(db)
    conn = SMBConnection(username=user, password=password, my_name="Alexander Brezhnev", remote_name="samba", use_ntlm_v2=True)

    sleep(5)

    if conn.connect(host, 445):
        with open(f'{from_path}{file}', 'rb') as my_file:
            conn.storeFile('dbs', f'{to_path}{file_result}', my_file)
    conn.close()

    sleep(5)


def transfer_files_to_click(files, path_to_file):
    import pandas as pd
    import pymysql
    import datetime
    import os
    import fsp.def_project_definition as def_project_definition
    from clickhouse_driver import Client
    from commons_liza import to_click


    timeout = 200

    full_calls = pd.read_csv(f'{path_to_file}/{files}')
    full_calls[['project', 'calldate', 'network_provider', 'count_good_calls_c', 'База', 'last_queue_c',
                'custom_queue_c', 'marker_c', 'town_c', 'city_c', 'category_calls', 'category', 'stop_auto',
                'Разговоры',
                'Звонки', 'Переводы', 'Заявки','region_c2']] = full_calls[['project', 'calldate',
                                                               'network_provider', 'count_good_calls_c', 'База',
                                                               'last_queue_c',
                                                               'custom_queue_c', 'marker_c', 'town_c', 'city_c',
                                                               'category_calls', 'category',
                                                               'stop_auto', 'Разговоры', 'Звонки', 'Переводы',
                                                               'Заявки','region_c2']].astype('str').fillna('')
    full_calls['calldate'] = pd.to_datetime(full_calls['calldate'])
    full_calls['marker_c'] = full_calls['marker_c'].astype(str)
    full_calls['marker_c'] = full_calls['marker_c'].apply(lambda x: x.replace('.0', ''))
    full_calls['category'] = full_calls['category'].astype(str)
    full_calls['category'] = full_calls['category'].apply(lambda x: x.replace('.0', ''))
    full_calls['city_c'] = full_calls['city_c'].astype(str)
    full_calls['city_c'] = full_calls['city_c'].apply(lambda x: x.replace('.0', ''))
    full_calls['stop_auto'] = full_calls['stop_auto'].astype(str)
    full_calls['stop_auto'] = full_calls['stop_auto'].apply(lambda x: x.replace('.0', ''))
    full_calls[['project', 'calldate', 'network_provider', 'count_good_calls_c', 'База', 'last_queue_c',
                'custom_queue_c', 'marker_c', 'town_c', 'city_c', 'category_calls', 'category', 'stop_auto',
                'Разговоры',
                'Звонки', 'Переводы', 'Заявки','region_c2']] = full_calls[['project', 'calldate',
                                                               'network_provider', 'count_good_calls_c', 'База',
                                                               'last_queue_c',
                                                               'custom_queue_c', 'marker_c', 'town_c', 'city_c',
                                                               'category_calls', 'category',
                                                               'stop_auto', 'Разговоры', 'Звонки', 'Переводы',
                                                               'Заявки','region_c2']].astype('str').fillna('')
    full_calls = full_calls.rename(columns={'База': 'base',
                                            'Разговоры': 'talks', 'Звонки': 'calls', 'Переводы': 'perevod',
                                            'Заявки': 'meeting'}).fillna('')

    full_calls['talks'] = full_calls['talks'].astype('int64')
    full_calls['calls'] = full_calls['calls'].astype('int64')
    full_calls['perevod'] = full_calls['perevod'].astype('int64')
    full_calls['meeting'] = full_calls['meeting'].astype('int64')
    full_calls = full_calls.fillna('').groupby(['project', 
                            'calldate', 
                            'network_provider',
                            'count_good_calls_c',
                            'base', 'last_queue_c',
                            'custom_queue_c', 'marker_c', 
                            'town_c', 'city_c', 
                            'category_calls', 'category', 
                            'stop_auto','region_c2'],as_index=False, dropna=False).agg({'talks': 'sum',
                                                    'calls': 'sum',
                                                    'perevod': 'sum',
                                                    'meeting': 'sum'})
    
    
    full_calls[['talks','calls','perevod','meeting']] = full_calls[['talks','calls','perevod','meeting']].astype('int64').fillna(0)

    print(full_calls.shape[0])
    print(full_calls['calldate'].min())


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
        date_i = full_calls['calldate'].min()

        print('Отправляем запрос')
        cluster = '{cluster}'
        sql_request = f'''ALTER TABLE nakopitelny_nedozvons ON CLUSTER '{cluster}' DELETE WHERE toDate(calldate) = '{date_i}' '''
        client.execute(sql_request)
        print('Данные удалены')
        time.sleep(timeout)
        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.nakopitelny_nedozvons VALUES', full_calls)
    except:
       
        print('Данные не загружены')
    finally:

        client.connection.disconnect()
        print('conection closed')

        




