def dayly_report_to_clickhouse(access_pass): 
    import pandas as pd
    import numpy as np
    from sqlalchemy import create_engine
    from sqlalchemy import text
    import datetime
    from sqlalchemy import sql
    from clickhouse_driver import Client
    import glob
    # import os
    # import sqlalchemy
    # import mysql
    # from mysql.connector import connect  # , Error
    # import mysql.connector
    # import pymysql
        
    # acces = {'db_host': '192.168.1.183',
    #      'db_name': 'suitecrm',
    #      'db_password': 'ZQzbW1g8UWYxuP9',
    #      'db_port': '',
    #      'db_user': 'VManetin'}

    with open(access_pass, "r", encoding="utf-8") as f:
        access_f = f.read()
    f.close()
    access_f = access_f.split('\n')
    access = {}
    for i in access_f:
        access[i.split(': ')[0]] = i.split(': ')[1]
   
    print('Выгружаем данные SQL запросом')
    sql_query = sql.text("""select DATE(c.date_entered) as date,
        c.assigned_user_id AS id,
        queue_c,
        c.duration_minutes,
        cc.result_call_c,
        CASE WHEN cc.otkaz_c IS NULL OR cc.otkaz_c = '' THEN 'null_status_otkaz' ELSE cc.otkaz_c END AS otkaz_c,
        COUNT(asterisk_caller_id_c) AS count_phone
    FROM calls c
    LEFT JOIN calls_cstm cc ON c.id = cc.id_c
    WHERE DATE(c.date_entered) >= DATE(DATE_SUB(NOW(), INTERVAL DAYOFMONTH(NOW())-1 DAY)) AND DATE(c.date_entered) <= DATE(DATE_SUB(NOW(), INTERVAL 1 DAY)) 
    AND c.direction = 'Inbound'
    GROUP BY date, assigned_user_id, queue_c, duration_minutes, result_call_c, otkaz_c
    ORDER BY date;""")

    # с 1 ч тек мес по вчера DATE(c.date_entered) >= DATE(DATE_SUB(NOW(), INTERVAL DAYOFMONTH(NOW())-1 DAY)) AND DATE(c.date_entered) <= DATE(DATE_SUB(NOW(), INTERVAL 1 DAY)) 
    # ORDER BY date_entered DESC
    # DATE(c.date_entered) >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) за вчера
    # DATE(c.date_entered) = DATE(NOW()) за сегодня

    sql_alchemy_conn_str = f"mysql+mysqldb://{access['db_user']}:{access['db_password']}@{access['db_host']}/{access['db_name']}"
    sql_alchemy_engine = create_engine(sql_alchemy_conn_str, pool_recycle=3600)

    with sql_alchemy_engine.connect() as mysql_conn:
        df_zvonki = pd.read_sql(sql_query, mysql_conn)

    print('Кол-во строк в таблице в загруженной таблице df_zvonki, отметка 1', len(df_zvonki))
    #print('Сохраняем таблицу')
    #result_df.to_csv(f'{path_to_file_airflow}SQL_{file_name}', index=False)
    #print('Таблица сохранена')


    df_zvonki['duration_minutes'].replace(['None', np.nan], 0, inplace=True)
    df_zvonki['queue_c'].replace(['None', np.nan, '', 'NULL', ''], 0, inplace=True)
    df_zvonki['result_call_c'].replace(['None', '', 'NULL', np.nan], 'Нет статуса', inplace=True)
    
    print('>>> сумма звонков после SQL выгрузки', df_zvonki['count_phone'].sum())
    print('Таблица Звонки сформирована. Начинаем обработку')
    # df_zvonki = pd.read_csv(f'{path_to_file_airflow}SQL_{file_name}')
    # print('Открыли файл Звонки для анализа')

    """Забираем данные из файлов"""
    print('Открываем файл Пользователи')
    df_users = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/users.csv')
    print('Объединяем файл Пользователи и Звонки')
    df_zvonki = pd.merge(df_zvonki, df_users[['id', 'fio', 'supervisor']], how='left', on='id')
    df_zvonki.drop(columns=['id'], axis=1, inplace=True)
    df_zvonki['fio'].replace(['', 'NULL', np.nan], 'Не указан', inplace=True)
    df_zvonki['supervisor'].replace(['', 'NULL', np.nan], 'Не указан', inplace=True)
    
    
    # заменим статусы 'Причина отказа из таблицы decoding.xlsx
    print('Открываем файлdecoding_new.csv и объединяем и меняем статус в Звонках')
    df_decoding = pd.read_csv('/root/airflow/dags/beeline_lids/Files/decoding_new.csv')  # путь к отказам
    decoding_dict = dict(zip(df_decoding['name'], df_decoding['name_ru']))
    df_zvonki['otkaz_c'] = df_zvonki['otkaz_c'].map(decoding_dict)

    # заменим статусы результатов звонков в осн таблице
    print('заменим статусы результатов звонков в осн таблице')
    result_call = {'CallWait': 'Назначен перезвон', 'refusing': 'Отказ', 'MeetingWait': 'Назначена заявка', 'null_status': 'Нет статуса'}
    df_zvonki['result_call_c'] = df_zvonki['result_call_c'].map(result_call)
    df_zvonki['result_call_c'] = df_zvonki['result_call_c'].fillna('Нет статуса')

    # соединим все файлы в папке/root/airflow/dags/project_defenition/projects/teams 
    print('соединим все файлы в папке/root/airflow/dags/project_defenition/projects/teams')
    csv_files = glob.glob("/root/airflow/dags/project_defenition/projects/teams/*.csv")
    today = datetime.date.today().day -1
    csv_files = sorted(csv_files, reverse=False)
    csv_files = csv_files[-today:]
    df_lidi = pd.DataFrame()
    for file in csv_files:
        df_lidi = pd.concat([df_lidi, pd.read_csv(file)], ignore_index=True)
    df_lidi = df_lidi[['date', 'supervisor', 'project']]
    df_lidi.drop_duplicates(subset=None, keep='first', inplace=True)
    df_lidi = df_lidi.dropna(subset= ['date']) 
    df_zvonki['supervisor'].replace(['', 'NULL', np.nan], 'Не указан', inplace=True)
    df_lidi['project'].replace(['', 'NULL', np.nan], 'Не указан', inplace=True)
    df_lidi['project'] = df_lidi['project'].fillna('Не указан')

    print('Добавим в таблицу Звонки проекты и команды из объединенных файлов')
    print('Кол-во строк в таблице df_zvonki до этого действия после изменения типа  date', len(df_zvonki))
    # df_lidi['date'] = df_lidi['date'].astype('datetime64[ns]')
    df_zvonki = df_zvonki.merge(df_lidi, how='left', on=['date', 'supervisor'])
    df_zvonki['project'] = df_zvonki['project'].fillna('Не указан')
    df_zvonki['date'] = df_zvonki['date'].astype('datetime64[ns]')
    df_zvonki['duration_minutes'] = df_zvonki['duration_minutes'].astype(int)

    df_zvonki['project'].replace(['', 'NULL', np.nan], 'Не указан', inplace=True)    



    df_zvonki['project'] = df_zvonki['project'].fillna('Не указан')
    df_zvonki['result_call_c'] = df_zvonki['result_call_c'].fillna('Нет статуса')
    print('Кол-во строк в таблице df_zvonki до окончат группировки ', len(df_zvonki))
    print('Сделаем группировку в Звонках')
    print('>>> сумма звонков перед последней группировкой', df_zvonki['count_phone'].sum())
    df_zvonki = df_zvonki.groupby(['date', 'queue_c', 'duration_minutes', 'result_call_c', 'otkaz_c', 'fio', 'supervisor', 'project']).agg({'count_phone': 'sum'}).reset_index()
    
    print('Кол-во строк в таблице df_zvonki после окончат группировки ', len(df_zvonki))
    print('>>> сумма звонков после последней группировки', df_zvonki['count_phone'].sum())
    # df_zvonki.to_csv(f'{path_to_file_airflow}{file_name}', index=False)
    print('Таблица обработана и сохранена, начинаем загружать ее в clickhouse')
    # print('Таблица сохранена') 



    print('Подключаемся к серверу')
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
    
    """print('Удаляем таблицу')
    try:
        client.execute('drop table suitecrm_robot_ch.report_10_this_month_before_yest')
    except Exception as e:
        print('Таблица не существуе, удалять нечего')
    print('Создаем таблицу')

    # date,queue_c, duration_minutes, result_call_c, otkaz_c, fio, supervisor, project, team, count_phone
 
    # 'date', 'queue_c', 'duration_minutes', 'result_call_c', 'otkaz_c', 'fio', 'supervisor', 'project'])
    sql_create = '''create table suitecrm_robot_ch.report_10_this_month_before_yest
                    (
                        date               Date,
                        queue_c            Int64,
                        duration_minutes   Int64,
                        result_call_c      String,
                        otkaz_c            String,
                        fio                String,
                        supervisor         String,
                        project            String,
                        count_phone        Int64,
                    ) ENGINE = MergeTree
                        order by date'''
    client.execute(sql_create)"""

    client.execute(f'ALTER TABLE suitecrm_robot_ch.report_10_this_month_before_yest DELETE WHERE date >= toStartOfMonth(now());')  # toDate("{today}")
    # client.execute(f'DELETE FROM suitecrm_robot_ch.report_10_this_month_before_yest WHERE date >= toStartOfMonth(now());')  # toDate("{today}")
    
    print('Данные за текущий месяц удалены')
    
    print('Добавляем новые данные с нач. месяца по вчера')
    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.report_10_this_month_before_yest VALUES', df_zvonki)
    print('Таблица загружена в clickhouse')
    print('Минимальная дата в таблице - ', df_zvonki['date'].min())
    print('Максимальная дата в таблице - ', df_zvonki['date'].max())
    # print(df_zvonki.info())

    # print(df_zvonki)

    # df_zvonki.to_csv('/root/airflow/dags/report_10/report_files/Звонки_bd.csv', index=False)
#import datetime
# date = str(datetime.date.today())
# year_num = date.split('-')[0]
# month_num = date.split('-')[1]
# # day_num = date.split('-')[2]
# file_name = f'Звонки_{year_num}_{month_num}.csv'
# path_to_file_airflow = '/root/airflow/dags/report_10/report_files/'
# # path_to_file_airflow = 'report_10/report_files/'
# print(f'Путь к таблице {path_to_file_airflow}{file_name}')

# access_pass = '/root/airflow/dags/report_10/report_files/access.txt'
# dayly_report_to_clickhouse(access_pass)
