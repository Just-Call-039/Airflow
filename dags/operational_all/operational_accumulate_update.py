def update_operational(n, stop, path_to_sql, file_name,path_to_airflow,file_deleted,path_del):
    import pandas as pd
    import pymysql
    import datetime
    import os
    import fsp.def_project_definition as def_project_definition
    from clickhouse_driver import Client


    file_to_delete = f'{path_del}{file_deleted}'   # Замените на имя файла, который вы хотите удалить
    if os.path.exists(file_to_delete):
        os.remove(file_to_delete)
        print(f"Файл {file_to_delete} успешно удален.")
    else:
        print(f"Файл {file_to_delete} не существует.")


    team_project = def_project_definition.team_project()
    queue_project = def_project_definition.queue_project()


    # Вычисляем дату предыдущих 14 дней
    today = datetime.date.today()
    previous_date = today - datetime.timedelta(days=15)

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
    sql = f'''ALTER TABLE suitecrm_robot_ch.operational DELETE WHERE calldate >= toDate('{previous_date}') AND calldate < toDate('{today}')'''

    # Отправляем запрос
    client.execute(sql)

    for i in range(0, stop):
        print(f'start {i}')
        print(datetime.datetime.now() - datetime.timedelta(days=n))
        now = datetime.datetime.now() - datetime.timedelta(days=n)
        to_csv = f'{path_to_airflow}/{file_name}'
        to_save = f'{path_to_airflow}/Оперативный_архив_{now.strftime("%m_%d")}.csv'
        print('Подключаемся к mysql')
        dest = '/root/airflow/dags/not_share/cloud_my_sql_128.csv'
        if dest:
            with open(dest) as file:
             for now in file:
                now = now.strip().split('=')
                first, second = now[0].strip(), now[1].strip()
                if first == 'host':
                    host2 = second
                elif first == 'user':
                    user2 = second
                elif first == 'password':
                    password2 = second

        Con = pymysql.Connect(host=host2, user=user2, passwd=password2, db="suitecrm",
                        charset='utf8')

        path_to_sql = f'{path_to_sql}'
        print('Выгружаем данные')
        sql_file = open(path_to_sql, 'r', encoding='utf-8')
        sql = sql_file.read()
        sql = sql.replace('\ufeff', '')
        sql = sql.format(n)
        df = pd.read_sql_query(sql, Con)

        print('Соединяем и выводим итоговый проект')
        df['dialog'] = df['dialog'].astype('str')
        df['team'] = df['team'].astype('str')
        df['calldate'] = df['calldate'].astype('str')
        df = df.merge(team_project, how='left', left_on=['calldate', 'team'], right_on=['date', 'team'])
        df = df.merge(queue_project, how='left', left_on=['calldate', 'dialog'], right_on=['date', 'Очередь'])
        df['destination_project'] = df['destination_project'].fillna('0')
        df['team_project'] = df['team_project'].fillna('0')

        df['project'] = df.apply(lambda row: def_project_definition.project(row), axis=1)

    # print('Кол-во звонков в робот логе за все время')
    # click_sql = f'''select phone, if(calls > 7, 7, 0) category
    #        from (
    #             select phone, count(dialog) calls
    #             from suitecrm_robot_ch.jc_robot_log
    #             where last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
    #             and toDate(call_date) < toDate(today()) - interval {n} day
    #             group by phone) TT'''
    #
    # client = Client(host=host_ch, port='9000', user=user, password=password,
    #                database='suitecrm_robot_ch', settings={'use_numpy': True})
    #
    # print('Получаем категории из кликхауса')
    # category = pd.DataFrame(client.query_dataframe(click_sql))

        df_phone = df[['phone']].astype('str').drop_duplicates()

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

        print('Создаем таблицу')
        sql_create = '''create table suitecrm_robot_ch.temp_operational3
                        (
                            phone String
                        ) ENGINE = MergeTree
                            order by phone'''
        client.execute(sql_create)

        client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})


        print('Отправляем запрос и получаем категории')
        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.temp_operational3 VALUES', df_phone)

        click_sql = f'''select temp_operational3.phone as phone, case when calls between 1 and 5 then '1-5 calls'
           when calls between 6 and 10 then '6-10 calls'
               when calls between 11 and 15 then '11-15 calls'
                   when calls > 15 then '> 15 calls'
                       else '0' end category_calls,
       case when answer between 1 and 5 then '1-5 answers'
           when answer between 6 and 10 then '6-10 answers'
                   when answer > 10 then '> 10 answers'
                       else '0' end category
    from suitecrm_robot_ch.temp_operational3
         left join (select phone, count(date) calls
                    from (
                             select distinct concat('8',substring(dst, 2, 10)) phone,
                                             toDate(calldate)      date
                             from asteriskcdrdb_all.cdr_all
                             where toDate(calldate) between (toDate(now()) - interval (357+{n}) day) and (toDate(now()) - interval (1+{n}) day)
                               and concat('8',substring(dst, 2, 10)) in (select *
                                                             from suitecrm_robot_ch.temp_operational3)
                             ) tt
                    group by phone) cdr on temp_operational3.phone = cdr.phone
         left join (select phone, count(dialog) answer
                    from (
                             select phone,
                                    dialog
                             from suitecrm_robot_ch.jc_robot_log
                             where toDate(call_date) between (toDate(now()) - interval (357+{n}) day) and (toDate(now()) - interval (1+{n}) day)
                               and last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
                               and phone in (select *
                                                               from suitecrm_robot_ch.temp_operational3)
                             ) t
                    group by phone) jc on temp_operational3.phone = jc.phone
    '''

        client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

        category = pd.DataFrame(client.query_dataframe(click_sql))
        print(category.columns)

        print('Удаляем таблицу')
        client.execute('drop table suitecrm_robot_ch.temp_operational3')

        print('Цепим категори')
        df = df.merge(category, how='left', on='phone')

    # df['category'] = ''
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
        'last_step'], as_index=False, dropna=False).agg({'calls': 'sum', 'trafic1': 'sum', 'trafic': 'sum'}).rename(columns={'trafic': 'full_trafic','trafic1': 'trafic'})
        
        
        df[['project','dialog','destination_queue','calldate','client_status',
        'was_repeat','marker','route','source','perevod','region','holod',
        'city_c','otkaz','trunk_id','autootvet','category_stat','stretched',
        'category','category_calls','last_step']] =  df[['project',
                        'dialog','destination_queue','calldate','client_status',
                        'was_repeat','marker','route','source','perevod','region','holod',
                        'city_c','otkaz','trunk_id','autootvet','category_stat','stretched',
                        'category','category_calls','last_step']].astype('str').fillna('')
        

        df[['calls','trafic','full_trafic']] = df[['calls','trafic','full_trafic']].astype('int64').fillna(0)


        print('Сохраняем')
        df.to_csv(to_save, index=False)


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


        n += 1
        print(f'----------------- the end {i}')