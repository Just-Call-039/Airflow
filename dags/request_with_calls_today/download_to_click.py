


import pandas as pd
from clickhouse_driver import Client
import datetime
from commons_liza.to_click import my_connection

def download_to_click(path_folder, file_request):
    
    req_click = pd.read_csv(f'{path_folder}{file_request}')
    req_click = req_click[['project','request_date','request_hour',
                      'user','super','status','last_queue_c','district_c',
                      'city','queue','my_phone_work']]
    req_click['request_hour'] = req_click['request_hour'].astype('str').apply(lambda x: x.replace('.0',''))
    req_click['city'] = req_click['city'].astype('str').apply(lambda x: x.replace('.0',''))
    req_click['last_queue_c'] = req_click['last_queue_c'].astype('str').apply(lambda x: x.replace('.0',''))
    req_click['queue'] = req_click['queue'].astype('str').apply(lambda x: x.replace('.0',''))

    stat = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/Статусы заявок.csv',  sep=',', encoding='utf-8')

    req_click = req_click.merge(stat, how = 'left', on='status')
    print(f'Заявки после соединиения {req_click.shape[0]}')


    req_click = req_click.astype('str')
    req_click['request_date'] = pd.to_datetime(req_click['request_date'])


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
        # client = Client(host=host, port='9000', user=user, password=password,
        #                 database='suitecrm_robot_ch', settings={'use_numpy': True})
        client = my_connection()

        print('Удаляем таблицу')
        client.execute('truncate table suitecrm_robot_ch.jc_meeting_module')
    except (ValueError):
        print('Данные не удалены')
    finally:
        try:

            print('Создаем таблицу')
            sql_create = '''create table if not exists jc_meeting_module
            (
            project       String,
            request_date  Date,
            request_hour  Int64,
            user          String,
            super         String,
            status        String,
            last_queue_c  String,
            district_c    String,
            city          String,
            queue         String,
            my_phone_work String,
            req_status    String
            )
            engine = MergeTree
                ORDER BY request_date'''
            client.execute(sql_create)

        except (ValueError):
            print('Таблица не создана')
        finally:
            try:

                print('Отправляем запрос c заявками')
                client.insert_dataframe('INSERT INTO suitecrm_robot_ch.jc_meeting_module VALUES', req_click)
            except (ValueError):
                print('Данные не загружены')
    try:

        print('Удаляем таблицу')
        cluster = '{cluster}'
        client.execute(f'''truncate table users_meet on cluster '{cluster}' ''')

    except (ValueError):
            print('Данные не удалены')
    finally:
        try:

            print('Создаем таблицу')
            sql_create = '''create table if not exists suitecrm_robot_ch.users_meet
                        (
                        id          String,
                        fio         String,
                        team        String,
                        supervisor  String
                        )
                        engine = MergeTree ORDER BY id
            '''
            client.execute(sql_create)
        except (ValueError):
            print('Данные не загружены')
        finally:
        
            client.connection.disconnect()
            print('conection closed')

    users = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/users.csv',  sep=',', encoding='utf-8')
    users = users[['id','fio','team','supervisor']].astype('str')

    try:
        client = my_connection()
        # client = Client(host=host, port='9000', user=user, password=password,
        #             database='suitecrm_robot_ch', settings={'use_numpy': True})
        print('Отправляем запрос c заявками')
        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.users_meet VALUES', users)
    except (ValueError):
        print('Данные не зашружены')
    finally:
        
        client.connection.disconnect()
        print('conection closed')
    