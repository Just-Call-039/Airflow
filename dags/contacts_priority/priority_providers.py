def priority():
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

    sql_drop = '''drop table suitecrm_robot_ch.priority_providers'''
    client.execute(sql_drop)

    sql_create = '''create table suitecrm_robot_ch.priority_providers
                        (
                            city_code Nullable(String),
                            provider Nullable(String),
                            # region_c Nullable(String),
                            ttk Nullable(Int8),
                            mts Nullable(Int8),
                            rtk Nullable(Int8),
                            nbn Nullable(Int8),
                            dom Nullable(Int8),
                            bln Nullable(Int8)

                        ) ENGINE = TinyLog'''
    
    client.execute(sql_create)

