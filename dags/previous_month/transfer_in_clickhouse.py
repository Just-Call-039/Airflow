def to_click(path_file, calls):
    import pandas as pd
    import pymysql
    import gspread 
    from oauth2client.service_account import ServiceAccountCredentials
    import datetime
    import os
    from datetime import datetime, timedelta
    import glob
    from datetime import datetime
    from clickhouse_driver import Client
    import re
    import pandas as pd
    import pymysql
    import datetime
    import os
    import fsp.def_project_definition as def_project_definition
    from clickhouse_driver import Client
    import datetime
    import os
    import glob

    df = pd.read_csv(f'{path_file}/{calls}')
    # df['call_count'] = 0.0

    df[['id','name',
    'contactid','queue', 
    'user_call','super',
    'city','dialog',
    'completed_c']]=df[['id','name',
    'contactid','queue',
    'user_call','super',
    'city','dialog',
    'completed_c']].fillna('').astype('str')
    
    
    df[['call_sec','short_calls']] = df[['call_sec','short_calls']].fillna(0).astype('int64')
    df['call_date'] = pd.to_datetime(df['call_date'])
    df['call_count'] = df['call_count'].fillna(0).astype('float64')
    df['queue'] = df['queue'].apply(lambda x: x.replace('.0',''))
    df['queue'] = df['queue'].apply(lambda x: x.replace('.0',''))
    df['completed_c'] = df['completed_c'].apply(lambda x: x.replace('0','Оператором'))
    df['completed_c'] = df['completed_c'].apply(lambda x: x.replace('1','Клиентом'))
    df['dialog'] = df['dialog'].apply(lambda x: x.replace('.0',''))
    df['phone'] = df['phone'].astype('str').apply(lambda x: x.replace('.0',''))
    df['phone'] = df['phone'].astype('str').apply(lambda x: x.replace('.0',''))

    
    print('Соединяем с пользователями и выводим проекты')
    city = pd.read_csv('/root/airflow/dags/current_month_yesterday/Files/Город.csv',  sep=',', encoding='utf-8').fillna('').astype('str')
    df = df.merge(city, left_on = 'city', right_on = 'city_c', how = 'left').fillna('')
    users = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/users.csv',  sep=',', encoding='utf-8').fillna('')
    df = df.merge(users, left_on = 'user_call', right_on = 'id', how = 'left').fillna('')
    
    path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json' 

    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
    gs = gspread.authorize(credentials)

    table_name4 = 'Команды/Проекты'

    work_sheet4 = gs.open(table_name4)
    sheet4 = work_sheet4.worksheet('Лиды')
    data4 = sheet4.get_all_values() 
    headers4 = data4.pop(0) 
    lids = pd.DataFrame(data4, columns=headers4)


    path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json' 
    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
    gs = gspread.authorize(credentials)

    table_name4 = 'Команды/Проекты'

    work_sheet4 = gs.open(table_name4)
    sheet4 = work_sheet4.worksheet('JC')
    data4 = sheet4.get_all_values() 
    headers4 = data4.pop(0) 
    jc = pd.DataFrame(data4, columns=headers4)
    
    df =  df.merge(lids[['Проект','СВ CRM']], left_on = 'supervisor', right_on = 'СВ CRM', how = 'left').fillna('')
    df =  df.merge(jc[['Проект','CRM СВ']], left_on = 'supervisor', right_on = 'CRM СВ', how = 'left').fillna('')
    def update_project(row):
        if row['Проект_x'] == '':
            row['Проект_x'] = row['Проект_y']
        else:
            row['Проект_x']
    df.apply(lambda row: update_project(row), axis=1)
    df = df[['id_x',
             'call_date',
             'name',
             'contactid',
             'queue',
             'user_call',
             'super',
             'Город',
             'Область',
             'call_sec',
             'short_calls',
             'dialog',
             'completed_c',
             'fio','supervisor','Проект_x','call_count','phone']].rename(columns={'id_x': 'id',
                         'Город': 'city',
                'Область': 'town',
                         'Проект_x': 'project'})
    df_requests = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/request/Заявки.csv',  sep=',', encoding='utf-8').fillna('')
    print('Request done') 
    df_requests['request_date'] = pd.to_datetime(df_requests['request_date'])
    df_requests = df_requests[(df_requests['request_date'] >= '2024-02-01') & (df_requests['request_date'] <= pd.to_datetime('today'))]
    df_requests['request_date']=df_requests['request_date'].fillna('').astype('str')
    df_requests['my_phone_work']=df_requests['my_phone_work'].fillna('').astype('str')
    df_requests =df_requests[['request_date','user','status','district_c', 'my_phone_work']]
    df['call_date']=df['call_date'].fillna('').astype('str')
    df1 =  df.merge(df_requests, left_on = ['phone','user_call','call_date'], right_on = ['my_phone_work','user','request_date'], how = 'outer')
    df1['call_date'] = pd.to_datetime(df1['call_date'])
    df1['request_date'] = pd.to_datetime(df1['request_date'])
    df1[['id','name',
    'contactid','queue',
    'user_call','super',
    'city','town','dialog',
    'completed_c','fio','supervisor',
                         'project','user',
                         'status','district_c',
                         'my_phone_work']]=df1[['id','name',
    'contactid','queue',
    'user_call','super',
    'city','town','dialog',
    'completed_c','fio','supervisor',
                         'project','user',
                         'status','district_c',
                         'my_phone_work']].fillna('').astype('str')
    df1 = df1[['id',
    'call_date',
    'name',
    'contactid'     ,
    'queue'         ,
    'user_call'     ,
    'super'         ,
    'city'          ,
    'town',
    'call_sec'      ,
    'short_calls'   ,
    'dialog'        ,
    'completed_c'   ,
    'fio'           ,
    'supervisor'    ,
    'project'       ,
    'call_count'    ,
    'request_date'  ,
    'user'          ,
    'status'        ,
    'district_c'    ,
    'my_phone_work' ]]



    print('Подключаемся к clickhouse')
    # dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
    # if dest:
    #             with open(dest) as file:
    #                 for now in file:
    #                     now = now.strip().split('=')
    #                     first, second = now[0].strip(), now[1].strip()
    #                     if first == 'host':
    #                         host = second
    #                     elif first == 'user':
    #                         user = second
    #                     elif first == 'password':
    #                         password = second
    #     # return host, user, password
 
    try:
        # client = Client(host=host, port='9000', user=user, password=password,
        #                database='suitecrm_robot_ch', settings={'use_numpy': True})
        client = to_click.my_connection()
        # cluster = '{cluster}'
        # Удаляем данные из таблицы



        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.pokazateli_operatorov_arhive VALUES', df1)
    except (ValueError):
        print('Данные не загружены')
    finally:

        client.connection.disconnect()
        print('conection closed')
    
     