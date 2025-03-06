from clickhouse_driver import Client
import pandas as pd
import requests

# Возвращает датафрейм старый клик

def get_data_ch(sql_download, date_before, date_i, file_path):
    
    client = Client(host='192.168.1.99', port = '9000', user='user1', password='r_e4hFHUfgtd32',
                        database='suitecrm_robot_ch', settings={'use_numpy': True})
                        
    sql_request = open(sql_download, "r", encoding='utf8', errors='ignore').read().format(date_before, date_i)

    df = pd.DataFrame(client.query_dataframe(sql_request)).fillna('0')
    df.fillna('0').to_csv(file_path, index = False)
    
def get_data_ch_187(sql_request, file_path):
    
    client = Client(host='192.168.1.174', user='user1', password='r_e4hFHUfgtd32',
                        database='suitecrm_robot_ch', settings={'use_numpy': True})

    df = pd.DataFrame(client.query_dataframe(sql_request)).fillna('0')
    df.fillna('0').to_csv(file_path, index = False)

    

# Создание таблицы на кликхаусе
        
def request_ch(sql_request):
    
    try:

        client = Client(host='192.168.1.174', user='user1', password='r_e4hFHUfgtd32',
                        database='suitecrm_robot_ch', settings={'use_numpy': True})
    
        client.execute(sql_request)
    
        print('request have done')
  
    except (ValueError):
        print('Данные не выгружены')
        
    finally:

        client.connection.disconnect()
        print('conection closed')
        
# Удаление таблицы с обеих нод

def delete_ch(sql_request):
    
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

                    client = Client(host=host, user='user1', password='r_e4hFHUfgtd32',
                            database='suitecrm_robot_ch', settings={'use_numpy': True})
                    
                    client.execute(sql_request)

            except (ValueError):

                print('Host dead')

            finally:

                client.connection.disconnect()
                print('conection closed')        

# Создание таблицы на клике с репликами
def create_table_ch(sql_request):
    
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
                    break

            except (ValueError):

                print('Host dead')
    try:

        client = Client(host=ch_host, user='user1', password='r_e4hFHUfgtd32',
                        database='suitecrm_robot_ch', settings={'use_numpy': True})
    
        client.execute(sql_request)
    
        print('table created')
  
    except (ValueError):
        print('Данные не выгружены')
        
    finally:

        client.connection.disconnect()
        print('conection closed')



def save_data(table_name, df):
    


    click_hosts = {
        "1": "192.168.1.191",
        "2": "192.168.1.174"
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


        print('Начинаем загрузку')
        
        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.{} VALUES'.format(table_name), df)
        print('ok')
        
    except (ValueError):
        
        print('Данные не загружены')
        
    finally:

        client.connection.disconnect()
        print('conection closed')
    