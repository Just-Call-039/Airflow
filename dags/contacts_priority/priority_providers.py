def priority_providers(file):
    import pandas as pd
    from clickhouse_driver import Client

    providers_priority = pd.read_csv(file, sep=';')


    print('Правим')
    providers_priority[['ptv_c','ttk', 'mts', 'rtk', 'nbn','dom', 'bln']] = providers_priority[['ptv_c','ttk', 'mts', 'rtk', 'nbn','dom', 'bln']].fillna(0).astype('int')
    providers_priority[['city_c', 'code_region', 'provider','region_c']] = providers_priority[['city_c', 'code_region', 'provider','region_c']].fillna('0').astype('str')

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

    print('Удаляем и пересоздаем таблицу')
    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    sql_create = '''create table suitecrm_robot_ch.priority_providers
                    (
                    ptv_c Int8,
                    city_c Nullable(String),
                    code_region Nullable(String),
                    provider Nullable(String),
                    region_c Nullable(String),
                    ttk Nullable(Int8),
                    mts Nullable(Int8),
                    rtk Nullable(Int8),
                    nbn Nullable(Int8),
                    dom Nullable(Int8),
                    bln Nullable(Int8)
                    
                    ) ENGINE = MergeTree
                        order by ptv_c'''

    client.execute('''drop table suitecrm_robot_ch.priority_providers''')
    client.execute(sql_create)

    print('Загружаем данные')

    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.priority_providers VALUES', providers_priority)