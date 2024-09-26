def operational_transformation(path_to_users, name_users, path_to_folder, name_calls, path_to_final_folder):
    import pandas as pd
    import glob
    import os
    import datetime
    import fsp.def_project_definition as def_project_definition
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
        # return host, user, password


    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})


    # Формируем SQL-запрос для удаления строк
    sql = f'''ALTER TABLE suitecrm_robot_ch.operational DELETE WHERE calldate = toDate(today()) OR calldate = toDate(yesterday())'''

    # Отправляем запрос
    client.execute(sql)

    df = pd.read_csv(f'{path_to_folder}/{name_calls}')
    df_phone = df[['phone']].astype('str').drop_duplicates()

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

    print('Подключаемся к серверу')
    client = Client(host=host, port='9000', user=user, password=password,
                database='suitecrm_robot_ch', settings={'use_numpy': True})
    
    print('Проверка застрявшей таблицы')

    if pd.DataFrame(client.execute('''show tables from suitecrm_robot_ch where table = 'temp_operational'  ''')).shape[0] == 0:
        print('Таблицы нет') 
    else:
        print('Удаляю застрявшую')
        client.execute('drop table suitecrm_robot_ch.temp_operational')

    print('Создаем таблицу')
    sql_create = '''create table suitecrm_robot_ch.temp_operational
                    (
                        phone String
                    ) ENGINE = MergeTree
                        order by phone'''
    client.execute(sql_create)

    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    print('Отправляем запрос и получаем категории')
    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.temp_operational VALUES', df_phone)


    click_sql = '''select temp_operational.phone as phone_work, case when calls between 1 and 5 then '1-5 calls'
           when calls between 6 and 10 then '6-10 calls'
               when calls between 11 and 15 then '11-15 calls'
                   when calls > 15 then '> 15 calls'
                       else '0' end category_calls,
       case when answer between 1 and 5 then '1-5 answers'
           when answer between 6 and 10 then '6-10 answers'
                   when answer > 10 then '> 10 answers'
                       else '0' end category
from suitecrm_robot_ch.temp_operational
         left join (select phone, count(date) calls
                    from (
                             select distinct concat('8',substring(dst, 2, 10)) phone,
                                             toDate(calldate)      date
                             from asteriskcdrdb_all.cdr_all
                             where toDate(calldate) between (toDate(now()) - interval 357 day) and (toDate(now()) - interval 1 day)
                               and concat('8',substring(dst, 2, 10)) in (select *
                                                             from suitecrm_robot_ch.temp_operational)
                             ) tt
                    group by phone) cdr on temp_operational.phone = cdr.phone
         left join (select phone, count(dialog) answer
                    from (
                             select phone,
                                    dialog
                             from suitecrm_robot_ch.jc_robot_log
                             where toDate(call_date) between (toDate(now()) - interval 357 day) and (toDate(now()) - interval 1 day)
                               and last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
                               and phone in (select *
                                                               from suitecrm_robot_ch.temp_operational)
                             ) t
                    group by phone) jc on temp_operational.phone = jc.phone'''
    
    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})
    category = pd.DataFrame(client.query_dataframe(click_sql))

    print('Удаляем таблицу')
    client.execute('drop table suitecrm_robot_ch.temp_operational')

    print('Пользователи')
    teams = pd.read_csv(f'{path_to_users}/{name_users}').fillna('')
    teams = teams[['id', 'team']]

    team_project = def_project_definition.team_project()
    queue_project = def_project_definition.queue_project()
    team_project['date'] = team_project['date'].astype('str')
    team_project['team'] = team_project['team'].astype('str')
    queue_project['date'] = queue_project['date'].astype('str')
    queue_project['Очередь'] = queue_project['Очередь'].astype('str')


    now = datetime.datetime.now()

    print('Соединяем и выводим итоговый проект')
    df['dialog'] = df['dialog'].astype('str')
    df['team'] = df['team'].astype('str')
    df['calldate'] = df['calldate'].astype('str')
    df = df.merge(team_project, how='left', left_on=['calldate', 'team'], right_on=['date', 'team'])
    df = df.merge(queue_project, how='left', left_on=['calldate', 'dialog'], right_on=['date', 'Очередь'])
    df['destination_project'] = df['destination_project'].fillna('0')
    df['team_project'] = df['team_project'].fillna('0')

    df['project'] = df.apply(lambda row: def_project_definition.project(row), axis=1)

    df['phone'] = df['phone'].astype('str').apply(lambda x: x.replace('.0',''))
    category['phone_work'] = category['phone_work'].astype('str').apply(lambda x: x.replace('.0',''))
    df = df.merge(category, how='left', left_on='phone', right_on='phone_work')

    print(df.columns)

    print('Группируем')
    df = df.groupby(['project',
        'dialog',
        'destination_queue',
        'calldate',
        'client_status',
        'was_repeat',
        'marker',
        'route',
        'source',
        'type_steps',
        'region',
        'holod',
        'city_c',
        'otkaz',
        'trunk_id',
        'autootvet',
        'category_stat',
        'stretched',
        'category',
        'category_calls',
        'last_step','region_c2'], as_index=False, dropna=False).agg({'calls': 'sum',
                                                         'trafic1': 'sum',
                                                         'trafic': 'sum'}).rename(columns={'trafic': 'full_trafic',
                                                                                           'trafic1': 'trafic'})
    print('Проверка ЕТВ')
    etv = pd.read_csv('/root/airflow/dags/operational_all/Files/operational/ЕТВ.csv',  sep=',', encoding='utf-8').fillna('').astype('str')
    df = df.merge(etv, how='left', left_on='dialog', right_on='queue').fillna('').astype('str')
    df['have_ptv_1'] = df['have_ptv_1'].astype('str').apply(lambda x: x.replace('.0',''))
    df['have_ptv_2'] = df['have_ptv_2'].astype('str').apply(lambda x: x.replace('.0',''))
    df['have_ptv_3'] = df['have_ptv_3'].astype('str').apply(lambda x: x.replace('.0',''))
    df['have_ptv_4'] = df['have_ptv_4'].astype('str').apply(lambda x: x.replace('.0',''))
    df['have_ptv_5'] = df['have_ptv_5'].astype('str').apply(lambda x: x.replace('.0',''))
    df['have_ptv_6'] = df['have_ptv_6'].astype('str').apply(lambda x: x.replace('.0',''))
    df['have_ptv_7'] = df['have_ptv_7'].astype('str').apply(lambda x: x.replace('.0',''))

    df['etv'] = ''
    def check_conditions(row):
        if (row['was_repeat'] == '1') and (row['route'].find(row['have_ptv_1']) != -1):
            row['etv'] = '1'
        elif (row['was_repeat'] == '1') and (row['route'].find(row['have_ptv_2']) != -1):
            row['etv'] = '1'
        elif (row['was_repeat'] == '1') and (row['route'].find(row['have_ptv_3']) != -1):
            row['etv'] = '1'
        elif (row['was_repeat'] == '1') and (row['route'].find(row['have_ptv_4']) != -1):
            row['etv'] = '1'
        elif (row['was_repeat'] == '1') and (row['route'].find(row['have_ptv_5']) != -1):
            row['etv'] = '1'
        elif (row['was_repeat'] == '1') and (row['route'].find(row['have_ptv_6']) != -1):
            row['etv'] = '1'
        elif (row['was_repeat'] == '1') and (row['route'].find(row['have_ptv_7']) != -1):
            row['etv'] = '1'
        else:
            row['etv'] = '0'

    df.apply(check_conditions, axis=1)
    df[['project','dialog','destination_queue','calldate','client_status',
        'was_repeat','marker',

        'source','type_steps','region','holod',
        'city_c','otkaz','trunk_id','autootvet','category_stat','stretched',
        'category','category_calls','last_step','etv','region_c2']] =  df[['project',
                        'dialog','destination_queue','calldate','client_status',
                        'was_repeat','marker',
                        'source','type_steps','region','holod',
                        'city_c','otkaz','trunk_id','autootvet','category_stat','stretched',
                        'category','category_calls','last_step','etv','region_c2']].astype('str').fillna('')

    df[['calls','trafic','full_trafic']] = df[['calls','trafic','full_trafic']].astype('int64').fillna(0)

    print('Группируем для выгрузки')
    df = df.groupby(['project',
        'dialog',
        'destination_queue',
        'calldate',
        'client_status',
        'was_repeat',
        'marker',
        'source',
        'type_steps',
        'region',
        'holod',
        'city_c',
        'otkaz',
        'trunk_id',
        'autootvet',
        'category_stat',
        'stretched',
        'category',
        'category_calls',
        'last_step',
        'etv','region_c2'], as_index=False, dropna=False).agg({'calls': 'sum',
                                                         'trafic': 'sum',
                                                         'full_trafic': 'sum'})
    

    df['calldate'] = pd.to_datetime(df['calldate'])

    df[['calls','trafic','full_trafic']] = df[['calls','trafic','full_trafic']].astype('int64').fillna(0)
    df['trunk_id'] = df['trunk_id'].astype('str').apply(lambda x: x.replace('.0',''))
    df['was_repeat'] = df['was_repeat'].astype('str').apply(lambda x: x.replace('.0',''))
    df['type_steps'] = df['type_steps'].astype('str').apply(lambda x: x.replace('.0',''))
    df['etv'] = df['etv'].astype('str').apply(lambda x: x.replace('.0',''))


    print('Сохраняем')
    df.to_csv(f'{path_to_final_folder}/{name_calls}', sep=',', index=False, encoding='utf-8')
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
        # return host, user, password


    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})
        
    print('Отправляем запрос')
    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.operational VALUES', df)

