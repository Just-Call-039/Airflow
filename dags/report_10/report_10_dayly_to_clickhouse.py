def dayly_report_to_clickhouse(access_pass): 
    import pandas as pd
    import numpy as np
    from sqlalchemy import create_engine
    from sqlalchemy import text
    import datetime
    from sqlalchemy import sql
    
    import glob
    from commons_liza import to_click
    from time import sleep
    
    with open(access_pass, "r", encoding="utf-8") as f:
        access_f = f.read()
    f.close()
    access_f = access_f.split('\n')
    access = {}
    for i in access_f:
        access[i.split(': ')[0]] = i.split(': ')[1]

    start_month = str(datetime.datetime.today().replace(day=1).date())
    end_month = str(datetime.datetime.today().date())
    # start_month = str((datetime.datetime.today() - datetime.timedelta(days=45)).replace(day=1).date())
    # end_month = str((datetime.datetime.today() - datetime.timedelta(days=45)).replace(day=31).date())
    print(start_month)
    print(end_month)
    print('Выгружаем данные SQL запросом')
    sql_query = sql.text(f'''select DATE(c.date_entered) as date,
        c.assigned_user_id AS id,
        queue_c,
        c.duration_minutes,
        cc.result_call_c,
        CASE WHEN cc.otkaz_c IS NULL OR cc.otkaz_c = '' THEN 'null_status_otkaz' ELSE cc.otkaz_c END AS otkaz_c,
        COUNT(asterisk_caller_id_c) AS count_phone
    FROM suitecrm.calls c
    LEFT JOIN suitecrm.calls_cstm cc ON c.id = cc.id_c
    WHERE DATE(c.date_entered) BETWEEN '{start_month}' AND '{end_month}'
    AND c.direction = 'Inbound'
    GROUP BY date, assigned_user_id, queue_c, duration_minutes, result_call_c, otkaz_c
    ORDER BY date''')

    
    sql_alchemy_conn_str = f"mysql+mysqldb://{access['db_user']}:{access['db_password']}@{access['db_host']}/{access['db_name']}"
    sql_alchemy_engine = create_engine(sql_alchemy_conn_str, pool_recycle=3600)

    with sql_alchemy_engine.connect() as mysql_conn:
        df_zvonki = pd.read_sql(sql_query, mysql_conn)

    print('Кол-во строк в таблице в загруженной таблице df_zvonki, отметка 1', len(df_zvonki))

    df_zvonki['duration_minutes'].replace(['None', np.nan], 0, inplace=True)
    df_zvonki['queue_c'].replace(['None', np.nan, '', 'NULL', ''], 0, inplace=True)
    df_zvonki['result_call_c'].replace(['None', '', 'NULL', np.nan], 'Нет статуса', inplace=True)
    
    print('>>> сумма звонков после SQL выгрузки', df_zvonki['count_phone'].sum())
    print('Таблица Звонки сформирована. Начинаем обработку')

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
    df_zvonki = df_zvonki.fillna('').groupby(['date', 'queue_c', 'duration_minutes', 'result_call_c', 'otkaz_c', 'fio', 'supervisor', 'project']).agg({'count_phone': 'sum'}).reset_index()
    
    print('Кол-во строк в таблице df_zvonki после окончат группировки ', len(df_zvonki))
    print('>>> сумма звонков после последней группировки', df_zvonki['count_phone'].sum())
    
    print('Таблица обработана и сохранена, начинаем загружать ее в clickhouse')
    
    cluster = '{cluster}'
    
    sql_request = f''' ALTER TABLE suitecrm_robot_ch.report_10_this_month_before_yest ON CLUSTER '{cluster}' 
                        DELETE WHERE toDate(date) BETWEEN '{start_month}' AND '{end_month}' ;'''

    to_click.delete_data(sql_request)

    
    sleep(600)
    print('Данные за текущий месяц удалены')
    
    print('Добавляем новые данные с нач. месяца по вчера')
    print('Минимальная дата в таблице - ', df_zvonki['date'].min())
    print('Максимальная дата в таблице - ', df_zvonki['date'].max())
    print(df_zvonki.head())
    to_click.save_df('report_10_this_month_before_yest', df_zvonki)

    print('Таблица загружена в clickhouse')


