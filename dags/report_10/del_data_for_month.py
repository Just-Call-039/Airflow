import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import text
import sqlalchemy
import mysql
from mysql.connector import connect, Error
import mysql.connector
import pymysql
import datetime
from sqlalchemy import sql
import os
from clickhouse_driver import Client
import glob


print('Подключаемся к серверу')
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

client = Client(host=host, port='9000', user=user, password=password,
                database='suitecrm_robot_ch', settings={'use_numpy': True})


client.execute(f'ALTER TABLE suitecrm_robot_ch.report_10_this_month_before_yest DELETE WHERE date > toStartOfMonth(now());')  # toDate("{today}")
print('Данные за текущий месяц удалены')