def create_priority_table():
    import pandas as pd
    from clickhouse_driver import Client
    from commons_liza.to_click import my_connection
    
    # print('Подключаемся к clickhouse')
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
    
    # print('Удаляем приоритетную таблицу и пересоздаем')
    # client = Client(host=host, port='9000', user=user, password=password,
    #                 database='suitecrm_robot_ch', settings={'use_numpy': True})

    client = my_connection()

    # sql_drop = '''drop table suitecrm_robot_ch.contacts_priorities'''
    # client.execute(sql_drop)
    client.execute('TRUNCATE TABLE IF EXISTS suitecrm_robot_ch.contacts_priorities')

    sql_create = '''create table if not exists suitecrm_robot_ch.contacts_priorities
                        (
                            id_custom          String,
                            ptv                Nullable(Int8),
                            priority1          Nullable(String),
                            priority2          Nullable(String)

                        ) ENGINE = MergeTree'''
    client.execute(sql_create)