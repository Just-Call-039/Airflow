import pandas as pd
from clickhouse_driver import Client
import requests

# Подключение к клику
def my_connection():

    click_hosts = {
        # "1": "192.168.1.174",
        "2": "192.168.1.191"
    }

    USER = 'user1'  
    PASSWORD = 'r_e4hFHUfgtd32'  

    ch_host = None

    for host in click_hosts.values():

            response = requests.post(
                f'http://{host}:8123/',
                data='select 1',
                auth=(USER, PASSWORD)
            )
            try:

                if response.ok == True:

                    ch_host = host
                    break

            except (ValueError):

                print('Host dead')
    print(ch_host)
    return Client(host=ch_host, user='user1', password='r_e4hFHUfgtd32',
                    database='suitecrm_robot_ch', settings={'use_numpy': True})


# Подключение к 174 для временных таблиц

def my_connection_174():

   
    return Client(host='192.168.1.174', user='user1', password='r_e4hFHUfgtd32',
                    database='suitecrm_robot_ch', settings={'use_numpy': True})


# Загрузка данных на кликхаус, передаем путь к датафрейму и типы полей

def save_data(table_name, path_df, type_dict):
    
    import requests

    click_hosts = {
        # "1": "192.168.1.174",
        "2": "192.168.1.191"
    }

    USER = 'user1'  
    PASSWORD = 'r_e4hFHUfgtd32'  

    ch_host = None

    for host in click_hosts.values():

            response = requests.post(
                f'http://{host}:8123/',
                data='select 1',
                auth=(USER, PASSWORD)
            )
            try:

                if response.ok == True:

                    ch_host = host
                    break

            except (ValueError):

                print('Host dead')

    try:

        client = Client(host=ch_host, user='user1', password='r_e4hFHUfgtd32',
                        database='suitecrm_robot_ch', settings={'use_numpy': True})
        df = pd.read_csv(path_df, dtype = type_dict)
        print(df.info())
        print(df.shape[0])

        print('Начинаем загрузку')
        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.{} VALUES'.format(table_name), df)
        
    except (ValueError):
        
        print('Данные не загружены')
        
    finally:

        client.connection.disconnect()
        print('conection closed')

# Сохранение данных на клик, передаем датафрейм

def save_df(table_name, df):
    
    click_hosts = {
        # "1": "192.168.1.174",
        "2": "192.168.1.191"
    }

    USER = 'user1'  
    PASSWORD = 'r_e4hFHUfgtd32'  

    ch_host = None

    for host in click_hosts.values():

            response = requests.post(
                f'http://{host}:8123/',
                data='select 1',
                auth=(USER, PASSWORD)
            )
            try:

                if response.ok == True:

                    ch_host = host
                    print(ch_host)
                    try:
        
                        client = Client(host=ch_host, user='user1', password='r_e4hFHUfgtd32',
                                        database='suitecrm_robot_ch', settings={'use_numpy': True})
                        print('Начинаем загрузку')
                        
                        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.{} VALUES'.format(table_name), df)
                        print('Данные загружены')
                        
                    except:
                        
                        print('Данные не загружены')
                        break
                        
                    finally:

                        client.connection.disconnect()
                        print('Соединение закрыто')
                                    

            except (ValueError):

                print('Host dead')

# Удаление данных из кликхаус

def delete_data(sql_request):
    
    import requests

    click_hosts = {
        # "1": "192.168.1.174",
        "2": "192.168.1.191"
    }

    USER = 'user1'  
    PASSWORD = 'r_e4hFHUfgtd32'  

    ch_host = None

    for host in click_hosts.values():

            response = requests.post(
                f'http://{host}:8123/',
                data='select 1',
                auth=(USER, PASSWORD)
            )
            try:

                if response.ok == True:

                    ch_host = host
                    break

            except (ValueError):

                print('Host dead')

    try:

        client = Client(host=ch_host, user='user1', password='r_e4hFHUfgtd32',
                        database='suitecrm_robot_ch', settings={'use_numpy': True})

        print('Отправляем запрос')
        client.execute(sql_request)
        
    except (ValueError):
        
        print('Данные не удалены')
        
    finally:

        client.connection.disconnect()
        print('conection closed')


def delete_data_per_period(table_name, date_col, date_i):
    
    import requests

    click_hosts = {
        "1": "192.168.1.174",
        "2": "192.168.1.191"
    }

    USER = 'user1'  
    PASSWORD = 'r_e4hFHUfgtd32'  

    ch_host = None

    for host in click_hosts.values():

            response = requests.post(
                f'http://{host}:8123/',
                data='select 1',
                auth=(USER, PASSWORD)
            )
            try:

                if response.ok == True:

                    ch_host = host
                    try:

                        client = Client(host=ch_host, user='user1', password='r_e4hFHUfgtd32',
                                        database='suitecrm_robot_ch', settings={'use_numpy': True})

                        print('Отправляем запрос')
                        cluster = '{cluster}'
                        sql_request = f'''ALTER TABLE {table_name} ON CLUSTER '{cluster}' DELETE WHERE toDate({date_col}) >= '{date_i}' '''
                        print(sql_request)
                        client.execute(sql_request)
                        
                    except (ValueError):
                        
                        print('Данные не удалены')
                        
                    finally:

                        client.connection.disconnect()
                        print('conection closed')
                    break

            except (ValueError):

                print('Host dead')

    