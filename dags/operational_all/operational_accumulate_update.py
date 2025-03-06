def update_operational(n, stop, path_to_sql, file_name,path_to_airflow,file_deleted,path_del):
    import pandas as pd
    import pymysql
    import datetime
    import os
    import fsp.def_project_definition as def_project_definition
    import datetime
    import os
    import glob
    from operational_all.redactor_operational_calls import changefillna, check_conditions
    from commons_liza import to_click


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
    

    try:

        client = to_click.my_connection()

        print('Удаляем данные за последние 14 дней')
        cluster =  '{cluster}'

        # Формируем SQL-запрос для удаления строк
        sql = f'''ALTER TABLE operational ON CLUSTER '{cluster}' DELETE
                         WHERE calldate >= toDate('{previous_date}') AND calldate < toDate('{today}')  '''
        # Отправляем запрос
        client.execute(sql)
    except (ValueError):
        print('Данные не удалены')
    finally:

        client.connection.disconnect()
        print('conection closed')

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
        sql = sql.format(n=n)
        df = pd.read_sql_query(sql, Con)
        if not df.empty:
            print('Соединяем и выводим итоговый проект')
            df['dialog'] = df['dialog'].astype('str')
            df['team'] = df['team'].astype('str')
            df['calldate'] = df['calldate'].astype('str')
            df = df.merge(team_project, how='left', left_on=['calldate', 'team'], right_on=['date', 'team'])
            df = df.merge(queue_project, how='left', left_on=['calldate', 'dialog'], right_on=['date', 'Очередь'])
            df['destination_project'] = df['destination_project'].fillna('0')
            df['team_project'] = df['team_project'].fillna('0')

            df['project'] = df.apply(lambda row: def_project_definition.project(row), axis=1)



            df_phone = df[['phone']].astype('str').drop_duplicates()

            print('Подключаемся к clickhouse')
 
            try:
                

                client = to_click.my_connection()
            
                print('Проверка застрявшей таблицы')

                if pd.DataFrame(client.execute('''show tables from suitecrm_robot_ch where table = 'temp_operational3'  ''')).shape[0] == 0:
                    print('Таблицы нет') 
                else:
                    print('Удаляю застрявшую')
                    client.execute('drop table suitecrm_robot_ch.temp_operational3 ')
                print('Создаем таблицу')
                sql_create = '''create table suitecrm_robot_ch.temp_operational3
                        (
                            phone String
                        ) ENGINE = MergeTree
                            order by phone'''
                client.execute(sql_create)

            
            except (ValueError):
                print('Таблица не создана')
            finally:

                client.connection.disconnect()
                print('conection closed')

            try:

                client = to_click.my_connection()


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

                category = pd.DataFrame(client.query_dataframe(click_sql))
                print(category.columns)

                print('Удаляем таблицу')
                client.execute('drop table suitecrm_robot_ch.temp_operational3 ')

            except (ValueError):
                print('Данные не загружены, таблица не удалена')
            finally:

                client.connection.disconnect()
                print('conection closed')

            print('Цепим категори')
            df = df.merge(category, how='left', on='phone')
            csv_files = glob.glob('/root/airflow/dags/project_defenition/projects/steps/*.csv')
            dataframes  = []

            for file in csv_files:
                df1 = pd.read_csv(file)
                dataframes.append(df1)
            print('Цепим шаги')
            steps = pd.concat(dataframes)
            steps['step'] = steps['step'].astype(str).apply(lambda x: x.replace('.0',''))
            steps['ochered'] = steps['ochered'].astype(str).apply(lambda x: x.replace('.0',''))
            steps['date'] = pd.to_datetime(steps['date'])
            steps['type_steps'] = steps['type_steps'].astype('str').apply(lambda x: x.replace('.0',''))
            df['last_step'] = df['last_step'].astype(str).apply(lambda x: x.replace('.0',''))
            df['dialog'] = df['dialog'].astype(str).apply(lambda x: x.replace('.0',''))    
            df['calldate'] = pd.to_datetime(df['calldate'])
   
            df = df.merge(steps, left_on = ['last_step','dialog','calldate'], 
                          right_on = ['step','ochered','date'], how = 'left').fillna('')
        


   
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
                            'last_step','region_c2', 'adial_campaign_id'], as_index=False, dropna=False).agg({'calls': 'sum',
                                                                                'trafic1': 'sum',
                                                                                'trafic': 'sum'}).rename(columns={'trafic': 'full_trafic',
                                                                                                                'trafic1': 'trafic'})
            
            print('Объединяем с ЕТВ')

           
            
            etv = pd.read_csv('/root/airflow/dags/operational_all/Files/operational/ЕТВ.csv',  sep=',', encoding='utf-8').fillna('').astype('str')
            df = df.merge(etv, how='left', left_on='dialog', right_on='queue').fillna('').astype('str')

            col_list = ['have_ptv_1', 'have_ptv_2', 'have_ptv_3', 'have_ptv_4', 'have_ptv_5', 'have_ptv_6', 'have_ptv_7']

            for col in col_list:

                # Убираем .0 если возникла, приводим к строке и меняем значение '' на '100500' для коректной работы поиска значения в строке

                df[col] = df[col].astype('str').apply(lambda x: x.replace('.0','')).apply(changefillna)


            df['etv'] = '0'
            for col in col_list:
   
                df['etv'] = df.apply(lambda row: check_conditions(row['route'], row[col], row['etv']), axis=1)


            df[['project','dialog','destination_queue','calldate','client_status',
        'was_repeat','marker',

        'source','type_steps','region','holod',
        'city_c','otkaz','trunk_id','autootvet','category_stat','stretched',
        'category','category_calls','last_step','etv','region_c2', 'adial_campaign_id']] =  df[['project',
                        'dialog','destination_queue','calldate','client_status',
                        'was_repeat','marker',
                        'source','type_steps','region','holod',
                        'city_c','otkaz','trunk_id','autootvet','category_stat','stretched',
                        'category','category_calls','last_step','etv','region_c2', 'adial_campaign_id']].astype('str').fillna('')

            df[['calls','trafic','full_trafic']] = df[['calls','trafic','full_trafic']].astype('int64').fillna(0)

            print('Группируем для выгрузки в ClickHouse')
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
        'etv','region_c2', 'adial_campaign_id'], as_index=False, dropna=False).agg({'calls': 'sum',
                                                         'trafic': 'sum',
                                                         'full_trafic': 'sum'})
            df['calldate'] = pd.to_datetime(df['calldate'])

            df[['calls','trafic','full_trafic']] = df[['calls','trafic','full_trafic']].astype('int64').fillna(0)


            df['trunk_id'] = df['trunk_id'].astype('str').apply(lambda x: x.replace('.0',''))
            df['was_repeat'] = df['was_repeat'].astype('str').apply(lambda x: x.replace('.0',''))
            df['type_steps'] = df['type_steps'].astype('str').apply(lambda x: x.replace('.0',''))
            df['etv'] = df['etv'].astype('str').apply(lambda x: x.replace('.0',''))
            print('Сохраняем')
            df.to_csv(to_save, index=False)
            print(df.columns)
            
            df = df[['project', 'dialog', 'destination_queue', 'calldate', 'client_status',
                    'was_repeat', 'marker', 'source', 'type_steps', 'region', 'holod',
                    'city_c', 'otkaz', 'trunk_id', 'autootvet', 'category_stat',
                    'stretched', 'category', 'category_calls', 'last_step', 'etv',
                    'region_c2', 'calls', 'trafic', 'full_trafic', 'adial_campaign_id']]

            print('Подключаемся к clickhouse')
            
            try:
                
                client = to_click.my_connection()
            
                print('Отправляем запрос')
                client.insert_dataframe('INSERT INTO suitecrm_robot_ch.operational VALUES', df)
                
            except (ValueError):
                print('Данные не загружены, таблица не удалена')
            finally:

                client.connection.disconnect()
                print('conection closed')


            n += 1
            print(f'----------------- the end {i}')
        else:
            n += 1