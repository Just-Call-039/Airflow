import pandas as pd
from clickhouse_driver import Client
from commons_liza import to_click


def save_table(table_name, save_sql, result_path, type_dict):
 
 df = pd.read_csv(result_path, parse_dates = ['date'], dtype = type_dict)
 print(df.info())

#  df['search_sec'] = df['search_sec'].fillna(0).astype('int64')
#  df['sqltook_sec'] = df['sqltook_sec'].fillna(0).astype('int64')
#  df = pd.read_csv(result_path)
#  print(df.info())
#  print(df.found.unique())
#  date_parse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
# df = pd.read_csv('file.csv', parse_dates=['date_col'], date_parser=date_parse)


# Пробуем подключиться
 try:

    client = to_click.my_connection()
    
# Создаем таблицу 

    print(f'Create table {table_name} if not exists')
    request_sql = open(save_sql).read()
    print(request_sql)
   
    client.execute(request_sql)
    print('ok')

# Записываем новый данные в таблицу 

    client = to_click.my_connection()
    
    print(f'Insert table {table_name} to db')

    print(f'INSERT INTO suitecrm_robot_ch.{table_name} VALUES')

    client.insert_dataframe(f'INSERT INTO suitecrm_robot_ch.{table_name} VALUES', df)

 except (ValueError):

    print('download have not done ', ValueError)
 finally:

    client.connection.disconnect()
    print('conection closed')




# Функция удаления данных из табицы по условию

def delete_ch(table_name, case_request):
 
 try:
 
    client = to_click.my_connection()
    
# Удаляем данные в таблице по условию

    print(f'delete table {table_name}')  

    cluster = '{cluster}'
    client.execute(f'''ALTER TABLE suitecrm_robot_ch.{table_name} ON CLUSTER '{cluster}' DELETE WHERE {case_request} ''')

 except (ValueError):

    print('delete data have not done ', ValueError)

 finally:
    print('delete data done ')

    client.connection.disconnect()
    print('conection closed')





def truncate_ch(table_name):
 
 try:
 
    client = to_click.my_connection()
    
# УДаляем таблицу с контактами 

    print(f'delete table {table_name}') 
    cluster = '{cluster}' 
    client.execute(f'''TRUNCATE TABLE suitecrm_robot_ch.{table_name} ON CLUSTER '{cluster}' ''')

 except (ValueError):
    print('delete table have not done ', ValueError)

 finally:
    print('delete done')

    client.connection.disconnect()
    print('conection closed')

