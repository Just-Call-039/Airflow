import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import text
# import sqlalchemy
# import mysql
from mysql.connector import connect  # , Error
# import mysql.connector
# import pymysql
from datetime import datetime, timedelta
from sqlalchemy import sql
# import os
from clickhouse_driver import Client
import glob

print('Ложная проверка клика.')

# print('Подключаемся к серверу')
# dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
# if dest:
#     with open(dest) as file:
#         for now in file:
#             now = now.strip().split('=')
#             first, second = now[0].strip(), now[1].strip()
#             if first == 'host':
#                 host = second
#             elif first == 'user':
#                 user = second
#             elif first == 'password':
#                 password = second

# client = Client(host=host, port='9000', user=user, password=password,
#                 database='suitecrm_robot_ch', settings={'use_numpy': True})

# result = client.execute(f"SELECT date, COUNT(project) as row_count  FROM suitecrm_robot_ch.report_10_this_month_before_yest WHERE date  >= '2024-05-25' GROUP BY date ORDER BY date DESC;")  # toDate("{today}")  | SELECT toDate(now()); | toStartOfDay(today())
# print(result)
# [print(str(i[0]), i[1]) for i in result]