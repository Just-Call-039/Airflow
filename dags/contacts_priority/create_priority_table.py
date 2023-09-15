def create_priority_table():
    import pandas as pd
    from clickhouse_driver import Client
    
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


    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})
    
    print('Удаляем приоритетную таблицу и пересоздаем')
    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    sql_drop = '''drop table suitecrm_robot_ch.contacts_priorities'''
    client.execute(sql_drop)

    sql_create = '''create table suitecrm_robot_ch.contacts_priorities
                        (
                            id_custom          String,
                            ptv                Nullable(Int8),
                            priority1          Nullable(String),
                            priority2          Nullable(String)

                        ) ENGINE = TinyLog'''
    client.execute(sql_create)