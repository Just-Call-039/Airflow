def data_table(data_create, data_insert):
    import pandas as pd
    from clickhouse_driver import Client
    from datetime import datetime

    print('Подключаемся к clickhouse')
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

    print('Удаляем таблицу и пересоздаем')
    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    sql_drop = '''drop table suitecrm_robot_ch.data'''
    client.execute(sql_drop)

    sql_create = open(data_create).read().replace('п»ї','').replace('﻿','').replace('\ufeff','')
    client.execute(sql_create)

    print('Приводим к общему виду, и заливаем в таблицу')
    sql_insert = open(data_insert).read().replace('п»ї','').replace('﻿','').replace('\ufeff','')

    client.execute(sql_insert)