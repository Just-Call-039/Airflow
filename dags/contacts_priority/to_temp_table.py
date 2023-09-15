def temp_table(temp_create, temp_insert):
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

    print('Удаляем временную таблицу и пересоздаем')
    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    sql_drop = '''drop table suitecrm_robot_ch.contacts'''
    client.execute(sql_drop)

    sql_create = open(temp_create).read().replace('п»ї','').replace('﻿','').replace('\ufeff','')
    client.execute(sql_create)

    print('Приводим таблицу к общему виду, и заливаем во временную таблицу')
    sql_insert = open(temp_insert).read().replace('п»ї','').replace('﻿','').replace('\ufeff','')

    client.execute(sql_insert)