import pandas as pd
from clickhouse_driver import Client
import MySQLdb
import datetime
import time
import os
import glob
import numpy as np


ch_174 =  {'host' : '192.168.1.174', 
         'user' : 'user1', 
         'password' : 'r_e4hFHUfgtd32', 
         'datebase' : 'asteriskcdrdb_all'}

def get_call(sql_path, file_path, file_name):

    # Подключаемся к базе suitecrm

    conn = MySQLdb.Connect(host='192.168.1.182', 
                        user="base_dep_slave",
                        passwd="IyHBh9mDBdpg",
                        db="suitecrm",
                        charset='utf8')
    
    sql_request = open(sql_path).read()
    
    call = pd.read_sql_query(sql_request, conn)

    call.to_csv(f'{file_path}{file_name}', index = False)

    conn.close()

def get_trunk(sql_path, file_path, file_name):

    try:
        print('Подключаемся к серверу')
        client = Client(host='192.168.1.174', user='user1', password='r_e4hFHUfgtd32',
                        database='asteriskcdrdb_all', settings={'use_numpy': True})
        
        sql_request = open(sql_path).read()

        df = pd.DataFrame(client.query_dataframe(sql_request))
        print('df size: ', df.shape[0])
        
        df.to_csv(f'{file_path}{file_name}', index = False)
  
    except:

        print('Данные не выгружены')
        
    finally:

        client.connection.disconnect()
        print('conection closed')
    
