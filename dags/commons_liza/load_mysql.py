from sqlalchemy import create_engine
from sqlalchemy.sql import text
import requests
import pandas as pd

# Скрипт читает запрос sql из файла и сохраняет полученный датафрейм в файл

def get_data(sql_download, cloud, date_i, file_path):

    # Создание строки подключения для SQLAlchemy
    connection_string = f'mysql+pymysql://{cloud[0]}:{cloud[1]}@{cloud[2]}/{cloud[3]}?charset=utf8'
    print(connection_string)
    print(date_i)
    engine = create_engine(connection_string)
    sql_request = open(sql_download, "r", encoding='utf8', errors='ignore').read().format(date_i=date_i)

    # Использование контекстного менеджера для безопасного открытия и закрытия соединения
    with engine.connect() as connection:
        df = pd.read_sql_query(text(sql_request), connection).fillna('')
        
        df.to_csv(file_path, index = False)   

# Скрипт читатет запрос из файла sql, добавляя условие по дате в запрос и сохраняет датафрейм по переданному пути


def get_data_permonth(sql_download, cloud, date_i, date_before, file_path):

    # Создание строки подключения для SQLAlchemy
    connection_string = f'mysql+pymysql://{cloud[0]}:{cloud[1]}@{cloud[2]}/{cloud[3]}?charset=utf8'
    print(connection_string)
    engine = create_engine(connection_string)
    sql_request = open(sql_download, "r", encoding='utf8', errors='ignore').read().format(date_before = date_before, date_i = date_i)

    # Использование контекстного менеджера для безопасного открытия и закрытия соединения
    with engine.connect() as connection:
        df = pd.read_sql_query(text(sql_request), connection).fillna('')
        
        df.fillna('').to_csv(file_path, index = False)   


# Скрипт читает переданный запрос (строкой) и возвращает датафрейм

def get_data_request(sql_download, cloud):

    # Создание строки подключения для SQLAlchemy
    connection_string = f'mysql+pymysql://{cloud[0]}:{cloud[1]}@{cloud[2]}/{cloud[3]}?charset=utf8'
    print(connection_string)
    engine = create_engine(connection_string)
    

    # Использование контекстного менеджера для безопасного открытия и закрытия соединения
    with engine.connect() as connection:
        df = pd.read_sql_query(text(sql_download), connection).fillna('')
        
        return df

# Передаем запрос и сохраняем полученный датафрейм в файл

def save_data_request(sql_download, cloud, file_path):

    # Создание строки подключения для SQLAlchemy
    connection_string = f'mysql+pymysql://{cloud[0]}:{cloud[1]}@{cloud[2]}/{cloud[3]}?charset=utf8'
    print(connection_string)
    engine = create_engine(connection_string)
    

    # Использование контекстного менеджера для безопасного открытия и закрытия соединения
    with engine.connect() as connection:
        df = pd.read_sql_query(text(sql_download), connection).fillna('')
        
        df.to_csv(file_path, index = False)

# Скрипт читает запрос sql из файла и сохраняет полученный датафрейм в файл

def get_df(sql_download, cloud, date_i):

    # Создание строки подключения для SQLAlchemy
    connection_string = f'mysql+pymysql://{cloud[0]}:{cloud[1]}@{cloud[2]}/{cloud[3]}?charset=utf8'
    print(connection_string)
    print(date_i)
    engine = create_engine(connection_string)
    sql_request = open(sql_download, "r", encoding='utf8', errors='ignore').read().format(date_i)

    # Использование контекстного менеджера для безопасного открытия и закрытия соединения
    with engine.connect() as connection:
        df = pd.read_sql_query(text(sql_request), connection).fillna('')
        
        return df