import pandas as pd

import pymysql
from clickhouse_driver import Client


def connect_db(file):
    dest = None
    print(file)
    if file == 'Maria_db':
        dest = '/root/airflow/dags/not_share/Maria_db.csv'
    elif file == 'cloud_117':
        dest = '/root/airflow/dags/not_share/cloud_my_sql_117.csv'
    elif file == 'cloud_128':
        dest = '/root/airflow/dags/not_share/cloud_my_sql_128.csv'
    elif file == 'cloud_183':
        dest = '/root/airflow/dags/not_share/cloud_my_sql_183.csv'
    elif file == '72':
        dest = '/root/airflow/dags/not_share/Second_cloud_72.csv'
    elif file == 'Combat':
        dest = '/root/airflow/dags/not_share/Combat_server.csv'
    elif file == 'Click':
        dest = '/root/airflow/dags/not_share/ClickHouse.csv'
    elif file == 'Click2':
        dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
    elif file == 'Server_MySQL':
        dest = '/root/airflow/dags/not_share/Server_files_MySQL.csv'
    elif file == 'DBS':
        dest = '/root/airflow/dags/not_share/DBS.csv'
    elif file == 'Truby':
        dest = '/root/airflow/dags/not_share/cloud_my_sql_truby.csv'
    else:
        print('Неизвестный сервер.')

    if dest:
        with open(dest) as file:
            for now in file:
                now = now.strip().strip('"').split('=')
                first, second = now[0].strip(), now[1].strip()
                if first == 'host':
                    host = second
                elif first == 'user':
                    user = second
                elif first == 'password':
                    password = second
        return host, user, password


def download_data_request(cloud, path_sql_request, path_to_file):
    
    
    print('try read file cloud ', cloud)
    
    host, user, password = connect_db(cloud)
    print('try connection')
    my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                 db="suitecrm",
                                 charset='utf8')
    
    sql_request = open(path_sql_request).read()
    print('sql_request: ', sql_request)
    df = pd.read_sql_query(sql_request, my_connect)
    df.to_csv(path_to_file, index = False)
    print('download done& size: ',df.shape[0])
    print('dates: ', df['date_entered'].unique())

    my_connect.close()

def connect_ch(cloud_ch):
 
    if cloud_ch:
        with open(cloud_ch) as file:
            for now in file:
                now = now.strip().split('=')
                first, second = now[0].strip(), now[1].strip()
                if first == 'host':
                    host = second
                elif first == 'user':
                    user = second
                elif first == 'password':
                    password = second
                elif first == 'password':
                    port = second

    return host, user, password


def save_table(cloud_ch, table_name, path_to_file):
 
 column_list_str = ['uniqueid', 'dialog', 'server_number', 'event_type', 'step', 'sec']
 types_dict = {col: str for col in column_list_str}
 

 date_parse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')


 
 df = pd.read_csv(path_to_file, dtype = types_dict, parse_dates=['date_entered'], date_parser=date_parse)
 print(df.info())

 
#  Достаме данные для подключения

 host, user, password = connect_ch(cloud_ch)

 port = '9000'

# Пробуем подключиться
 try:
 
    client = Client(host=host, port=port, user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})
    

# Записываем новый данные в таблицу 

    client = Client(host=host, port=port, user=user, password=password,
                        database='suitecrm_robot_ch', settings={'use_numpy': True})
    
    print(f'Insert table {table_name} to db')
    

    client.insert_dataframe(f'INSERT INTO suitecrm_robot_ch.{table_name} VALUES', df)

 except:
    
    raise

 finally:

    client.connection.disconnect()
    print('conection closed')