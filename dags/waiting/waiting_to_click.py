import pandas as pd
from clickhouse_driver import Client
from indicators_to_regions.defs import del_point_zero 
from commons_liza.to_click import my_connection


def to_click(path_to_file, file_name):

# Загружаем датасет за пршлый день
 

    df = pd.read_csv(f'{path_to_file}{file_name}')

    col_list = ['last_step', 'ochered', 'caller_id', 'queue_num_curr']

    del_point_zero(df, col_list)

    # Отправляем в clickhous

    print('Подключаемся к clickhouse')

    # Достаем host, user & password
    # dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
    # if dest:
    #             with open(dest) as file:
    #                 for now in file:
    #                     now = now.strip().split('=')
    #                     first, second = now[0].strip(), now[1].strip()
    #                     if first == 'host':
    #                         host = second
    #                     elif first == 'user':
    #                         user = second
    #                     elif first == 'password':
    #                         password = second
    try:
        # # Записываем новый данные в таблицу userrefusal_call_previos
        # client = Client(host=host, port='9000', user=user, password=password,
        #                 database='suitecrm_robot_ch', settings={'use_numpy': True})
        client = my_connection()  
    # # Создаем таблицу waiter
    # print('Create table call')
    # sql_create = '''create table if not exists suitecrm_robot_ch.waiter
    # (
    # call_time            Date,
    # caller_id            String,
    # queue_num_curr       String,            
    # last_step            String,
    # ochered              String,
    # project_name         String,
    # )
    # engine = MergeTree ORDER BY call_time;'''
    # client.execute(sql_create)
    
    # Записываем новый данные в таблицу waiter

        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.waiter VALUES', df)
    except (ValueError):
        print('Данные не загружены')
    finally:

        client.connection.disconnect()
        print('conection closed')


    

