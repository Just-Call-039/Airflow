import pandas as pd
import gspread 
from oauth2client.service_account import ServiceAccountCredentials
from clickhouse_driver import Client
import pandas as pd
import datetime as dt
from current_month_yesterday import defs
from indicators_to_regions.download_googlesheet import download_gs
from route_robotlogs.all_functions import del_staple
from commons_liza.to_click import my_connection


def call_to_click(path_file, call):

    # Загружаем датасет со звонками 

    df = pd.read_csv(f'{path_file}/{call}')
    print('date since ', df['call_date'].min())

    # Заполняем пустые строковые nan

    df[['id',
        'name',
        'contactid',
        'queue',
        'user_call',
        'super',
        'city',
        'dialog',
        'completed_c']]  = df[['id',
                              'name',
                              'contactid',
                              'queue',
                              'user_call',
                              'super',
                              'city',
                              'dialog',
                              'completed_c']].fillna('').astype('str')
                                
    # Преобразуем типы для merge

    df[['call_sec','short_calls']] = df[['call_sec','short_calls']].fillna(0).astype('int64')
    df['call_date'] = pd.to_datetime(df['call_date'])
    df['call_count'] = df['call_count'].fillna(0).astype('float64')
    df['queue'] = df['queue'].apply(lambda x: x.replace('.0',''))
    df['queue'] = df['queue'].apply(lambda x: x.replace('.0',''))
    df['completed_c'] = df['completed_c'].apply(lambda x: x.replace('0.0','Оператором'))
    df['completed_c'] = df['completed_c'].apply(lambda x: x.replace('1.0','Клиентом'))
    df['name'] = df['name'].replace({'** Авто-запись **': 'auto',
                                                        'Входящий звонок': 'inbound',
                                                        'Исходящий звонок': 'outbound'})
    df['dialog'] = df['dialog'].apply(lambda x: x.replace('.0',''))
    df['phone'] = df['phone'].astype('str').apply(lambda x: x.replace('.0',''))
    df['phone'] = df['phone'].astype('str').apply(lambda x: x.replace('.0',''))

    # Объединяем датафрейм с городами
    print('start merge df & city')
    city = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv',  sep=',', encoding='utf-8').fillna('').astype('str')
    df = df.merge(city, left_on = 'city', right_on = 'city_c', how = 'left').fillna('')
    print('date since after merge ', df['call_date'].min())
    print('size df  ', df.shape[0])

    # Объединяем датафрейм с пользователями
    print('merge df & users')
    users = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/users.csv',  sep=',', encoding='utf-8').fillna('')
    df = df.merge(users, left_on = 'user_call', right_on = 'id', how = 'left').fillna('')
    print('date since after merge ', df['call_date'].min())
    print('size df ', df.shape[0])
  
    lids = download_gs('Команды/Проекты', 'Лиды')

    jc = download_gs('Команды/Проекты', 'JC')
    
    # merge с лидами
    print('merge df & lids')
    df =  df.merge(lids[['Проект','СВ CRM', 'МРФ']], left_on = 'supervisor', right_on = 'СВ CRM', how = 'left').fillna('')
    print('date since after merge ', df['call_date'].min())
    print('size df ', df.shape[0])

    # merge с проектами
    print('merge df & jc')
    df =  df.merge(jc[['Проект','CRM СВ']], left_on = 'supervisor', right_on = 'CRM СВ', how = 'left').fillna('')
    print('date since after merge ', df['call_date'].min())
    print('size df ', df.shape[0])

    # Заполняем поле проекты
    def update_project(row):
        if row['Проект_x'] == '':
            row['Проект_x'] = row['Проект_y']
        else:
            row['Проект_x']

    df.apply(lambda row: update_project(row), axis=1)

    # Оставляем нужные столбцы и переименовываем

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
             'fio',
             'supervisor',
             'Проект_x',
             'МРФ',
             'call_count',
             'phone']].rename(columns={'id_x': 'id',
                                        'Город': 'city',
                                        'Область': 'town',
                                        'Проект_x': 'project',
                                        'МРФ' : 'region'})
    
    # Загружаем заявки

    df_request = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/sql_total/request.csv',  sep=',', encoding='utf-8').fillna('')
    print('Request download') 

    # Меняем типы данных для дальнейшего merge
    df_request['request_date'] = pd.to_datetime(df_request['request_date'])
    df_request = df_request[(df_request['request_date'] >= '2024-02-01') & (df_request['request_date'] <= pd.to_datetime('today'))]

    print('size request ', df_request.shape[0])
    df_request['request_date']=df_request['request_date'].fillna('').astype('str')
    df_request['my_phone_work']=df_request['my_phone_work'].fillna('').astype('str')
    df_request = df_request[['request_date', 'user', 'status', 'district_c','my_phone_work']]
    df_request['NN'] = 1.0

    df =df.sort_values(['call_date', 'user_call', 'contactid', 'call_sec'])
    df['NN'] = df.groupby(['call_date', 'user_call', 'contactid', 'call_sec']).cumcount() + 1

    df['call_date']=df['call_date'].fillna('').astype('str')

    # Объединяем датафрейм с запросами
    print('start merge df_request')
    df =  df.merge(df_request, left_on = ['phone','user_call','call_date'], right_on = ['my_phone_work','user','request_date'], how = 'outer')
    print('date after merge with request', df['call_date'].unique())
    print('размер датасета  ', df.shape[0])

    df['call_date'] = pd.to_datetime(df['call_date'])
    df['request_date'] = pd.to_datetime(df['request_date'])
    

    df[[
        'name',
        'contactid',
        'queue',
        'user_call',
        'super',
        'city',
        'town',
        'dialog',
        'completed_c',
        'fio',
        'supervisor',
        'project',
        'region',
        'user',
        'status',
        'district_c',
        'my_phone_work']]=df[[
                               'name',
                                'contactid',
                                'queue',
                                'user_call',
                                'super',
                                'city',
                                'town',
                                'dialog',
                                'completed_c',
                                'fio',
                                'supervisor',
                                'project',
                                'region',
                                'user',
                                'status',
                                'district_c',
                                'my_phone_work']].fillna('').astype('str')
    
    df['city'] = df['city'].apply(defs.find_letter)
    df['town'] = df['town'].apply(defs.find_letter)


    df = df[['id',
    'call_date',
    'name',
    'contactid'     ,
    'queue'         ,
    'user_call'     ,
    'super'         ,
    'city'          ,
    'town'          ,
    'call_sec'      ,
    'short_calls'   ,
    'dialog'        ,
    'completed_c'   ,
    'fio'           ,
    'supervisor'    ,
    'project'       ,
    'region'        ,
    'call_count'    ,
    'request_date'  ,
    'user'          ,
    'status'        ,
    'district_c'    ,
    'my_phone_work' ]].rename(columns={ 'id' : 'CallId',
                                        'call_date' : 'CallDate',
                                        'name' : 'CallName',
                                        'contactid' : 'CallContactId',
                                        'queue'  : 'CallQueue',
                                        'user_call' : 'CallUserId',
                                        'super'  : 'CallSupervisorId',
                                        'city' : 'CallCity',
                                        'town' : 'CallTown',
                                        'call_sec' : 'CallSec',
                                        'short_calls' : 'CallShortCall',
                                        'dialog' : 'CallDialog',
                                        'completed_c' : 'CallCompleted',
                                        'fio' : 'CallFio',
                                        'supervisor' : 'CallSupervisor',
                                        'project' : 'CallProject',
                                        'region' : 'CallRegion',
                                        'call_count' : 'CallCountSec',
                                        'request_date' : 'RequestDate',
                                        'user' : 'RequestUser',
                                        'status' : 'RequestStatus',
                                        'district_c' : 'RequestDistrict',
                                        'my_phone_work' : 'RequestPhone' 
    })
    
    df_call = df.groupby(['CallDate', 'CallName', 'CallContactId', 'CallQueue', 'CallUserId', 'CallSupervisorId',\
                            'CallCity', 'CallTown', 'CallSec', 'CallShortCall', 'CallDialog', 'CallCompleted', 'CallFio', \
                            'CallSupervisor', 'CallProject', 'CallRegion', 'CallCountSec', 'RequestDate', 'RequestUser',\
                            'RequestStatus', 'RequestDistrict', 'RequestPhone'], dropna=False).agg({'CallId': ['count']}).reset_index()
    
    df_call.columns = df_call.columns.droplevel(1)
    df_call = df_call.rename(columns={'CallId' : 'CallCount'})
    print('df size after group ', df_call.shape[0])
    print('df count call after group ', df_call['CallCount'].sum())
    

# Отправляем в clickhous

    print('Подключаемся к clickhouse')
    
    try:
        
        client = my_connection()
    
        # # Очистим таблицу usermetric_call_today

        print('delete from table call_today')
        client.execute('TRUNCATE TABLE suitecrm_robot_ch.usermetric_call_today')
    except (ValueError):
        print('Данные не удалены')
    finally:
        try:

            # Создаем таблицу usermetric_call_today

            print('Create table call')
            sql_create = '''CREATE TABLE IF NOT EXISTS suitecrm_robot_ch.usermetric_call_today
                        (
                            CallDate            Date,
                            CallName            String,
                            CallContactId       String,
                            CallQueue           String,
                            CallUserId          String,
                            CallSupervisorId    String,
                            CallCity            String,
                            CallTown            String,
                            CallSec             Int64,
                            CallShortCall       Int64,
                            CallDialog          String,
                            CallCompleted       String,
                            CallFio             String,
                            CallSupervisor      String,
                            CallProject         String,
                            CallRegion          String,
                            CallCountSec        Float64,
                            RequestDate         Date,
                            RequestUser         String,
                            RequestStatus       String,
                            RequestDistrict     String,
                            RequestPhone        String,
                            CallCount           Int64 
                        )
                            engine = MergeTree ORDER BY CallDate;'''
            client.execute(sql_create)
        except (ValueError):
            print('Таблица не создана')
        finally:
            try:

                client.insert_dataframe('INSERT INTO suitecrm_robot_ch.usermetric_call_today VALUES', df_call)

            except (ValueError):
                print('Данные не загружены')
            finally:

                client.connection.disconnect()
                print('conection closed')


def call_10_to_click(path_file, call_10):

    # Загружаем таблицу со звонками 
     
    df_call = pd.read_csv(f'{path_file}{call_10}')
    print('date since ', df_call['dateCall'].min())

    # Объединяем датафрейм с пользователями
    print('merge df & users')
    df_user = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/users.csv',  sep=',', encoding='utf-8').fillna('')
    df_user = df_user.rename(columns = {'id' : 'userid'})
    df_union_user = df_call.merge(df_user[['userid', 'fio', 'supervisor', 'team']], left_on = 'userid', right_on = 'userid', how = 'left')
    df_union_user['duration_minutes'] = df_union_user['duration_minutes'].fillna('0')
    df_union_user['otkaz_c'] = df_union_user['otkaz_c'].fillna('0')
    print('date since after merge ',df_union_user['dateCall'].min())
    print('size df ', df_union_user.shape[0])

    df_lids = download_gs('Команды/Проекты', 'Лиды')

    df_jc = download_gs('Команды/Проекты', 'JC')

    # Объединяем таблицу звонкв с лидами
    print('merge df_union & lids')
    df_lids = df_lids.rename(columns = {'СВ CRM' : 'supervisor'})
    df_union_lids =  df_union_user.merge(df_lids[['Проект','supervisor', 'МРФ']], on = 'supervisor', how = 'left')
    df_union_lids['duration_minutes'] = df_union_lids['duration_minutes'].fillna('0')
    df_union_lids['otkaz_c'] = df_union_lids['otkaz_c'].fillna('0')
    df_union_lids = df_union_lids.fillna('')
    print('size df ', df_union_lids.shape[0])

    # Объединяем таблицу звонкв с jc
    print('merge df & jc')
    df_jc = df_jc.rename(columns = {'CRM СВ' : 'supervisor'})
    df_union_jc =  df_union_lids.merge(df_jc[['Проект','supervisor']], on = 'supervisor', how = 'left')
    df_union_jc['duration_minutes'] = df_union_jc['duration_minutes'].fillna('0')
    df_union_jc = df_union_jc.fillna('')
    print('date since after merge ', df_union_jc['dateCall'].min())
    print('size df ', df_union_jc.shape[0])

    df_union_jc['Проект'] = df_union_jc.apply(lambda row: defs.update_project(row['Проект_x'], row['Проект_y']), axis=1)
    del df_union_jc['Проект_x']
    del df_union_jc['Проект_y']

    # Оставляем нужные столбцы и переименовываем

    df_union_jc = df_union_jc[['dateCall',
                'userid',
                'queue_c',             
                'result_call_c',
                'otkaz_c',             
                'project_c',
                'city_c',
                'count(asterisk_caller_id_c)',
                'set_queue',
                'duration_minutes',
                'marker',
                'ptv',
                'fio',
                'supervisor',
                'Проект',
                'МРФ']].rename(columns={'Проект': 'project',
                                            'МРФ' : 'region'})
    
    df_decoding_otkaz = df_jc = pd.read_excel('/root/airflow/dags/current_month_yesterday/Files/decoding.xlsx', sheet_name = 'Лист1')
    df_decoding_otkaz = df_decoding_otkaz.rename(columns={'name':'otkaz_c'})

    df_union_otkaz =  df_union_jc.merge(df_decoding_otkaz, on = 'otkaz_c', how = 'left')
    df_union_otkaz['duration_minutes'] = df_union_otkaz['duration_minutes'].fillna('0')
    df_union_otkaz = df_union_otkaz.fillna('')
    print('date since after merge ', df_union_otkaz['dateCall'].min())
    print('size df ', df_union_otkaz.shape[0])

    # Объединяем датафрейм с городами
    print('start merge df & city')
    city = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv',  sep=',', encoding='utf-8').fillna('').astype('str')
    df_union_otkaz['city_c'] = df_union_otkaz['city_c'].fillna(0).astype(str)
    defs.del_point_zero(df_union_otkaz, ['city_c'])
    city['Город'] = city['Город'].fillna('').apply(defs.find_letter)
    city['Область'] = city['Область'].fillna('').apply(defs.find_letter)

    df_union_city = df_union_otkaz.merge(city, on = 'city_c', how = 'left')
    print(df_union_city['Город'].unique())
    df_union_city['duration_minutes'] = df_union_city['duration_minutes'].fillna('0')
    
    df_union_city = df_union_city.fillna('')
    
    # Объединим датафрейм с таблицей качество , чтобы извлечь название разметки

    df_quality = pd.read_csv('/root/airflow/dags/current_month_yesterday/Files/Качество.csv')
    df_quality['Качество города'] = df_quality['Качество города'].apply(del_staple)
    df_quality = df_quality.rename(columns = {'id' : 'ptv'})
    df_union_city = df_union_city.merge(df_quality[['ptv', 'Качество города']], on = 'ptv', how= 'left')
    del df_union_city['ptv']
    df_union_city = df_union_city.rename(columns = {'Качество города' : 'ptv'})

    print('size df  ', df_union_city.shape[0])

    df_union_city = df_union_city[[
                                    'dateCall',
                                    'userid',
                                    'queue_c',             
                                    'result_call_c',           
                                    'city_c',
                                    'count(asterisk_caller_id_c)',
                                    'set_queue',
                                    'duration_minutes',
                                    'marker',
                                    'ptv',
                                    'fio',
                                    'supervisor',
                                    'project',
                                    'region',
                                    'name_ru']].rename(columns={
                                                                'dateCall' : 'CallDate',
                                                                'userid' : 'CallUserID',
                                                                'queue_c' : 'CallQueue',             
                                                                'result_call_c' : 'CallResult',                        
                                                                'city_c' : 'CallCityCode',
                                                                'count(asterisk_caller_id_c)' : 'CallCountId',
                                                                'set_queue' : 'CallSetQueue',
                                                                'duration_minutes' : 'CallDurationMinute',
                                                                'marker' : 'CallMarker',
                                                                'ptv' : 'CallPTV',
                                                                'fio' : 'UserFio',
                                                                'supervisor' : 'UserSupervisor',
                                                                'project' : 'LidsProject',
                                                                'region' : 'JCRegion',
                                                                'name_ru' : 'OtkazName'
    
            })
    
    # Убирем лишние точки с нулями в значениях 
    df_union_city['CallDurationMinute'] = df_union_city['CallDurationMinute'].fillna('0')
    df_union_city = df_union_city.fillna('').astype('str')
    col_list = ['CallQueue', 'CallCityCode', 'CallSetQueue', 'CallDurationMinute', 'CallMarker']
    defs.del_point_zero(df_union_city, col_list)
    df_union_city['CallDurationMinute'] = df_union_city['CallDurationMinute'].astype('int64')


    # Отправляем в clickhous

    print('Подключаемся к clickhouse')

    # Достаем host, user & password
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

    try:                         
        # client = Client(host=host, port='9000', user=user, password=password,
        #             database='suitecrm_robot_ch', settings={'use_numpy': True})  
        client = my_connection() 
        # Очищаем таблицу call_today
        print('delete dates from table call_today')
        cluster = '{cluster}'
        client.execute(f'''truncate table userrefusal_call_today on cluster '{cluster}' ''')

    except (ValueError):
        print('Данные не удалены')

    finally:
        try:        
            # Создаем таблицу usermetric_call
            print('Create table call')
            sql_create = '''CREATE TABLE IF NOT EXISTS userrefusal_call_today
                        (
                        CallDate            Date,
                        CallUserID          String,
                        CallQueue           String,            
                        CallResult          String,
                        CallCityCode        String,
                        CallCountId         Int64,
                        CallSetQueue        String,
                        CallDurationMinute  String,
                        CallMarker          String,
                        CallPTV             String,
                        UserFio             String,
                        UserSupervisor      String,
                        LidsProject         String,
                        JCRegion            String,
                        OtkazName           String
                        )
                        engine = MergeTree ORDER BY CallDate;'''
            client.execute(sql_create)
        except (ValueError):
            print('Таблица не создана')
        finally:
            try:
                # Записываем новый данные в таблицу usermetric_call
                client.insert_dataframe('INSERT INTO userrefusal_call_today VALUES', df_union_otkaz)
            except (ValueError):
                print('Данные не загружены')
            finally:
                client.connection.disconnect()
                print('conection closed')
 

