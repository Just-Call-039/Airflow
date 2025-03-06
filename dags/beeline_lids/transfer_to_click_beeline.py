def beeline_clickhouse(path_to_files, calls, work, otkaz):
    import pandas as pd
    import pymysql
    import gspread 
    from oauth2client.service_account import ServiceAccountCredentials
    import datetime
    import os
    import glob
    from datetime import datetime
    from clickhouse_driver import Client

    calls = pd.read_csv(f'{path_to_files}/{calls}')
    work = pd.read_csv(f'{path_to_files}/{work}')
    otkaz = pd.read_csv(f'{path_to_files}/{otkaz}')

    beeline_calls=calls.fillna('')
    work_time=work.fillna(0)
    work_time['dorabotka_talk'] = work_time['dorabotka_talk'].astype(str)
    work_time['dorabotka_talk'] = work_time['dorabotka_talk'].apply(lambda x: x.replace('.0',''))
    work_time['time_start_status'] = work_time['time_start_status'].astype(str)
    work_time['date'] = work_time['date'].astype(str)
    work_time['time_stop_status'] = work_time['time_stop_status'].astype(str)
    users = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/users.csv',  sep=',', encoding='utf-8')


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

    beeline_calls = beeline_calls.merge(users[['id','supervisor']], how='left', left_on='user_call', right_on='id')
    beeline_calls= beeline_calls.merge(lids[['СВ CRM','Проект']], how='left', left_on='supervisor', right_on='СВ CRM')
    beeline_calls = beeline_calls[beeline_calls['Проект'] == "BEELINE LIDS"]
    beeline_calls = beeline_calls.merge(otkaz[['name','name_ru']], how='left', left_on='otkaz', right_on='name')
    beeline_calls['end_talk'] = beeline_calls['end_talk'].astype(str)
    beeline_calls['end_talk'] = beeline_calls['end_talk'].apply(lambda x: x.replace('0 days ',''))
    beeline_calls['call_sec'] = beeline_calls['call_sec'].astype(str)
    beeline_calls['call_sec'] = beeline_calls['call_sec'].apply(lambda x: x.replace('.0','')).fillna('0')
    beeline_calls['call_date'] = pd.to_datetime(beeline_calls['call_date'])

    beeline_calls = beeline_calls[['id_x', 'call_date','start_talk', 'end_talk', 'name_x', 'contactid',
                                    'queue','dialog','user_call', 'city',
                                    'call_sec','completed','login_user',
                                    'result_call','name_ru','supervisor']].rename(columns={'id_x': 'id','name_x': 'name',
                                                                'name_ru': 'otkaz'})
    beeline_calls[['id','start_talk','end_talk','name','contactid','queue','dialog',
                'user_call','city','call_sec','completed','login_user','result_call',
                'otkaz','supervisor']] = beeline_calls[['id','start_talk','end_talk','name','contactid','queue','dialog',
                'user_call','city','call_sec','completed','login_user','result_call','otkaz','supervisor']].fillna('').astype('str')
    beeline_calls['queue'] = beeline_calls['queue'].astype(str)
    beeline_calls['queue'] = beeline_calls['queue'].apply(lambda x: x.replace('.0','')).fillna('0')
    beeline_calls['dialog'] = beeline_calls['dialog'].astype(str)
    beeline_calls['dialog'] = beeline_calls['dialog'].apply(lambda x: x.replace('.0','')).fillna('0')
    beeline_calls['city'] = beeline_calls['city'].astype(str)
    beeline_calls['city'] = beeline_calls['city'].apply(lambda x: x.replace('.0','')).fillna('0')

    print(beeline_calls[beeline_calls['id'] == '442f3e8a-330f-11ef-b4ed-ac162d7725d8'])


    work_time = work_time.merge(users[['id','supervisor']], how='left', left_on='id_user', right_on='id')
    work_time= work_time.merge(lids[['СВ CRM','Проект']], how='left', left_on='supervisor', right_on='СВ CRM')
    work_time = work_time[work_time['Проект'] == "BEELINE LIDS"]
    work_time = work_time[['id_user', 'user_name','date', 'talk_inbound', 'talk_outbound', 'ozhidanie',
                                'obrabotka','training', 'nastavnik',
                                'sobranie','problems','obuchenie',
                                'dorabotka','pause','lunch_duration','dorabotka_talk','start_status',
                                'stop_status','time_start_status','time_stop_status','supervisor']]


    work_time[['id_user','user_name','dorabotka_talk','start_status','stop_status','time_start_status','time_stop_status',
            'supervisor']] = work_time[['id_user','user_name','dorabotka_talk','start_status','stop_status','time_start_status','time_stop_status',
            'supervisor']] .fillna('').astype('str')
    work_time[['talk_inbound','talk_outbound','ozhidanie','obrabotka','training','nastavnik','sobranie',
            'problems','obuchenie','dorabotka','pause','lunch_duration']] = work_time[['talk_inbound','talk_outbound','ozhidanie','obrabotka','training','nastavnik','sobranie',
            'problems','obuchenie','dorabotka','pause','lunch_duration']] .fillna(0).astype('int64')


    print('Подключаемся к серверу')
    dest = '/root/airflow/dags/not_share/ClickHouseBeeline.csv'
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

    try:
        client = Client(host=host, port='9000', user=user, password=password,
                    database='beeline', settings={'use_numpy': True})
   
        print('Добавляем журнал звонков за предыдущий день')
        client.insert_dataframe('INSERT INTO beeline.beeline_calls VALUES',
                            beeline_calls)
    except (ValueError):
        print('Данные не загружены')

    finally:

        try:
    
            print('Добавляем данные по рабочему времени')
            client.insert_dataframe('INSERT INTO beeline.beeline_work_time VALUES',
                            work_time)
    
        except (ValueError):
            print('Данные не загружены')

        finally:

            client.connection.disconnect()
            print('conection closed')


