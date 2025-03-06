from sqlalchemy import create_engine
from sqlalchemy.sql import text
import requests
import pandas as pd


def get_data(sql_download, cloud, date_i, file_path):

    # Создание строки подключения для SQLAlchemy
    connection_string = f'mysql+pymysql://{cloud[0]}:{cloud[1]}@{cloud[2]}/{cloud[3]}?charset=utf8'
    print(connection_string)
    engine = create_engine(connection_string)
    sql_request = open(sql_download, "r", encoding='utf8', errors='ignore').read().format(date_i)

    # Использование контекстного менеджера для безопасного открытия и закрытия соединения
    with engine.connect() as connection:
        df = pd.read_sql_query(text(sql_request), connection).fillna('0')
        
        df.fillna('0').to_csv(file_path, index = False)   


def get_data_permonth(sql_download, cloud, date_i, date_before, file_path):

    # Создание строки подключения для SQLAlchemy
    connection_string = f'mysql+pymysql://{cloud[0]}:{cloud[1]}@{cloud[2]}/{cloud[3]}?charset=utf8'
    print(connection_string)
    engine = create_engine(connection_string)
    sql_request = open(sql_download, "r", encoding='utf8', errors='ignore').read().format(date_before, date_i)

    # Использование контекстного менеджера для безопасного открытия и закрытия соединения
    with engine.connect() as connection:
        df = pd.read_sql_query(text(sql_request), connection).fillna('0')
        
        df.fillna('0').to_csv(file_path, index = False)   

def get_robotlog(sql_download, cloud, date_i, date_before, file_path):

    # Создание строки подключения для SQLAlchemy
    connection_string = f'mysql+pymysql://{cloud[0]}:{cloud[1]}@{cloud[2]}/{cloud[3]}?charset=utf8'
    print(connection_string)
    engine = create_engine(connection_string)

    sql_request = open(sql_download, "r", encoding='utf8', errors='ignore').read().format(start=str(date_before), end=str(date_i))

    # Использование контекстного менеджера для безопасного открытия и закрытия соединения
    with engine.connect() as connection:
        df = pd.read_sql_query(text(sql_request), connection).fillna('0')
        
        df.fillna('0').to_csv(file_path, index = False)  
