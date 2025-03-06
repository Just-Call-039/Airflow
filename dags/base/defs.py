
import pandas as pd
from clickhouse_driver import Client
import pymysql
from commons_liza import to_click
from commons_sawa import connect_db


def del_staple(x):
    
    if x != '':
        x = x[1:]
        x = x[:-1]
        for letter in x:
            if letter != ' ':
                x = x[1:]
            else:
                return x[1:]
    else:
        return x

def region_c(x):
    for i in x:
        if i == '1':
            return 'Наша полная'
        elif i == '2':
            return 'Наша не полная'
        elif i == '3':
            return 'ПТВ в карте'
        elif i == '4':
            return 'Фиас из разных источников'
        elif i == '5':
            return 'Фиас до города'
        elif i == '6':
            return 'Старый town_c'
        elif i == '7':
            return 'Def-code'
        elif i == 'Пусто':
            return 'Пусто'
        else:
            return 'Пусто'


def network_provider_c(i):
    if i in {'10','68'}:
        return 'Теле 2'
    elif i == '80':
        return 'Билайн'
    elif i == '82':
        return 'Мегафон'
    elif i == '83':
        return 'МТС'
    elif i == 'Пусто':
        return 'Пусто'
    else:
        return 'MVNO'
    
def phone(x):
    if len(x) == 11:
        if x.startswith('89'):
            return 'Мобильный'
        elif x.startswith('8'):
            return 'Городской'
        else:
            return 'Неизвестно'
    else:
        return 'Неизвестно'
    

def download_to_click(table_name, sql_download, df):
 
 try:
 
    client = to_click.my_connection()
    
# Создаем таблицу data

    print(f'Create table {table_name}')

    sql_request = open(sql_download, "r", encoding='utf8', errors='ignore').read()
    
    client.execute(sql_request)

# Записываем новый данные в таблицу usermetric_call
    
    print(f'insert table {table_name}')
    # df = pd.read_csv(path_df)

    client.insert_dataframe(f'INSERT INTO suitecrm_robot_ch.{table_name} VALUES', df)
 except (ValueError):
    print('Данные не загружены ', ValueError)
 finally:

    client.connection.disconnect()
    print('conection closed')

def delete_ch(table_name):
 

 try:
 
    client = to_click.my_connection()
    
# УДаляем таблицу с контактами 

    print(f'delete table {table_name}')  

    cluster = '{cluster}'
    client.execute(f'''TRUNCATE TABLE {table_name} ON CLUSTER '{cluster}' ''')

 except (ValueError):
    print('Данные не удалены ', ValueError)

 finally:
    print('Данные удалены ')
    client.connection.disconnect()
    print('conection closed')

def download_data(cloud, path_sql_file):
    
    print('try read file cloud ', cloud)
    
    host, user, password = connect_db(cloud)
    print('try connection')
    my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                 db="suitecrm",
                                 charset='utf8')

    my_query = open(path_sql_file, "r", encoding='utf8', errors='ignore').read().replace('п»ї','').replace('﻿','').replace('\ufeff','')

    df = pd.read_sql_query(my_query, my_connect)

    my_connect.close()

    return df

def fillnan_my(x):
    if (x == '') | (x == ' '):
        return '0'
    else:
        return x
