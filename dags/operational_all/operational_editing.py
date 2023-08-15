def operational_transformation(path_to_users, name_users, path_to_folder, name_calls, path_to_final_folder):
    import pandas as pd
    import glob
    import os
    import datetime
    import fsp.def_project_definition as def_project_definition
    from clickhouse_driver import Client

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
        'perevod',
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
        'last_step'], as_index=False, dropna=False).agg({'calls': 'sum','trafic': 'sum'})
        # .rename(columns={'category_y': 'category'})

    print('Сохраняем')

    df.to_csv(f'{path_to_final_folder}/{name_calls}', sep=',', index=False, encoding='utf-8')

