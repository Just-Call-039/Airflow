import pandas as pd
from clickhouse_driver import Client
import pandas as pd
from current_month_yesterday import defs
from indicators_to_regions.download_googlesheet import download_gs
from route_robotlogs.all_functions import del_staple
from commons_liza.to_click import my_connection

def call_to_click(path_file, call):


    # Загружаем датасет со звонками 

    df = pd.read_csv(f'{path_file}/{call}')
    print('date since ', df['call_date'].min())
    print('unique name ', df['name'].unique())

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

    # print('test_operator ')
    # print(df[(df['user_call'] == '9f363445-d5e1-05ed-ad8d-642563f79a4a') & (df['call_date'] == '2024-09-01')]\
    #         [['contactid', 'call_count', 'city']])

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

    # Загрузим датасеты с лидами и проектами
       
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
    df =  df.merge(df_request, left_on = ['phone', 'user_call', 'call_date', 'NN'], right_on = ['my_phone_work', 'user', 'request_date', 'NN'], how = 'outer')
    print('date after merge with request', df['call_date'].unique())
    print('размер датасета  ', df.shape[0])
   
    df['call_date'] = pd.to_datetime(df['call_date'])
    df['request_date'] = pd.to_datetime(df['request_date'])

    
    df[['name',
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
        'my_phone_work']]=df[[  'name',
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
    print(df_call.columns)
    
    df_call = df_call.rename(columns={'CallId' : 'CallCount'})
   
    print('df size after group ', df_call.shape[0])
    print('df count call after group ', df_call['CallCount'].sum())
    

# Отправляем в clickhous

    print('Подключаемся к clickhouse')

    # # Достаем host, user & password
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
        #                 database='suitecrm_robot_ch', settings={'use_numpy': True})
        client = my_connection()
        
        # Очистим таблицу usermetric_call_current

        print('delete from table call')
        client.execute('truncate table suitecrm_robot_ch.usermetric_call_current')
    except (ValueError):
            print('Данные не удалены')
    finally:
        try:
            # Создаем таблицу usermetric_call
            print('Create table call')
            sql_create = '''create table if not exists suitecrm_robot_ch.usermetric_call_current
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

                client.insert_dataframe('INSERT INTO suitecrm_robot_ch.usermetric_call_current VALUES', df_call)
            except (ValueError):
                print('Данные не загружены')
            finally:

                client.connection.disconnect()
                print('conection closed')

def work_to_click(path_file, work_hour):

    # Выгружаем датафрейм с рабочими часами
    df_work = pd.read_csv(f'{path_file}{work_hour}', sep = ';')

    # Создадим столбец с общим временем
    
    def total_sec(row):
        col_list = ['talk_inbound', 'talk_outbound', 'ozhidanie', 'obrabotka', 'training', 'nastavnik', 'sobranie', \
                                   'problems', 'obuchenie', 'dorabotka']
        result = 0
        for col in col_list:
              result += row[col]
        return result
    
    df_work['total_sec'] = df_work.apply(lambda row: total_sec(row), axis=1)

    # Создадим столбец с полезным временем

    def effective_worktime(row):
        col_list = ['talk_inbound', 'talk_outbound', 'ozhidanie', 'obrabotka']
        result = 0
        for col in col_list:
              result += row[col]
        return result

    df_work['effective_worktime'] = df_work.apply(lambda row: effective_worktime(row), axis=1)

    # Создадим столбец с неполезным временем

    def uneffective_worktime(row):
        col_list = ['sobranie', 'dorabotka', 'pause', 'lunch_duration']
        result = 0
        for col in col_list:
              result += row[col]
        return result

    df_work['uneffective_worktime'] = df_work.apply(lambda row: uneffective_worktime(row), axis=1)

    # Переименуем столбцы для выгрузки в clickhous
    df_work = df_work.rename(columns={
         'id_user': 'WorktimeIdUser',
         'date' : 'WorktimeDate',
         'talk_inbound' : 'WorktimeTalkInbound',
         'talk_outbound' : 'WorktimeTalkOutbound',
         'ozhidanie' : 'WorktimeOzhidanie',
         'obrabotka' : 'WorktimeObrabotka',
         'training' : 'WorktimeTraining',
         'nastavnik' : 'WorktimeNastavnik',
         'sobranie' : 'WorktimeSobranie',
         'problems' : 'WorktimeProblems',
         'obuchenie' : 'WorktimeObuchenie',
         'dorabotka' : 'WorktimeDorabotka',
         'pause' : 'WorktimePause',
         'lunch_duration' : 'WorktimeLunchDuration',
         'dorabotka_talk' : 'WorktimeDrabotkaTalk',
         'total_sec' : 'WorktimeTotalSec',
         'effective_worktime' : 'WorktimeEffectiveTime',
         'uneffective_worktime' : 'WorktimeUneffectiveTime'})
    
    df_work.to_csv('/root/airflow/dags/current_month_yesterday/Files/work_test.csv')
    # Подключаемся к базе
    print('connect to clickhouse')

    # Достаем host, user, password
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
        #                 database='suitecrm_robot_ch', settings={'use_numpy': True})
        client = my_connection()
        #  Очищаем таблицу usermetric_worktime_current
        print('drop dates from table')
        cluster = '{cluster}'
        client.execute(f'''truncate table usermetric_worktime_current on cluster '{cluster}' ''')
    except (ValueError):
        print('Данные не удалены')
    finally:
        try:        
            print('create table')
            sql_create_work = '''create table if not exists usermetric_worktime_current
            
                            (   WorktimeIdUser              String,
                                WorktimeDate                Date,
                                WorktimeTalkInbound         Int64,
                                WorktimeTalkOutbound        Int64,
                                WorktimeOzhidanie           Int64,
                                WorktimeObrabotka           Int64,
                                WorktimeTraining            Int64,
                                WorktimeNastavnik           Int64,
                                WorktimeSobranie            Int64,
                                WorktimeProblems            Int64,
                                WorktimeObuchenie           Int64,
                                WorktimeDorabotka           Int64,
                                WorktimePause               Int64,
                                WorktimeLunchDuration       Int64,
                                WorktimeDrabotkaTalk        Int64,
                                WorktimeTotalSec            Int64,
                                WorktimeEffectiveTime       Int64,
                                WorktimeUneffectiveTime     Int64
                            )
                                engine = MergeTree ORDER BY WorktimeDate;'''
            client.execute(sql_create_work)
        except (ValueError):
            print('Таблица не создана')
        finally:
            try:

                # Записываем новые данные в таблицу
                client.insert_dataframe('INSERT INTO suitecrm_robot_ch.usermetric_worktime_current VALUES', df_work)
            except (ValueError):
                print('Данные не загружены')
            finally:

                client.connection.disconnect()
                print('conection closed')


def usertotal_to_click(path_file, usertotal):

    # Выгружаем датафрейм с рабюочими часами
    df_usertotal = pd.read_csv(f'{path_file}{usertotal}')
    df_usertotal = df_usertotal.fillna('')
    df_usertotal['first_workday_c'] = df_usertotal['first_workday_c'].apply(lambda x: x.replace('0000-00-00', ''))
    df_usertotal['penalty_c'] = df_usertotal['penalty_c'].fillna(0).astype(str).apply(lambda x: x.replace('.0', ''))
    df_usertotal['sip'] = df_usertotal['sip'].fillna(0).astype(str).apply(lambda x: x.replace('.0', ''))


    df_usertotal = df_usertotal.rename(columns={'id' : 'OperatorAddId',
                                 'date_entered' : 'OperatorAddDateEntered',
                                 'first_workday_c' : 'OperatorAddFirstWorkDay',
                                 'dismissal_date_c' : 'OperatorAddDismissalDate',
                                 'last_login_c' : 'OperatorAddLastLogin',
                                 'user_id_c' : 'OperatorAddUserId',
                                 'penalty_c' : 'OperatorAddPenalty',
                                 'sip' : 'OperatorAddSip'
    })

    # Подключаемся к базе
    print('connect to clickhouse')

    # Достаем host, user, password
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
        #  Очищаем данные из таблицы operatoradd
        print('delete dates from table')
        cluster = '{cluster}'
        client.execute(f'''truncate table usermetric_operatoradd on cluster '{cluster}' ''')
    except (ValueError):
        print('Данные не удалены')
    finally:
        try:
            # Создаем таблицу operatoradd
            print('create table')
            sql_create_work = '''create table if not exists usermetric_operatoradd
            
                            (   OperatorAddId               String,
                                OperatorAddDateEntered      Date,
                                OperatorAddFirstWorkDay     Date,
                                OperatorAddDismissalDate    Date,
                                OperatorAddLastLogin        Date,
                                OperatorAddUserId           String,
                                OperatorAddPenalty          String,
                                OperatorAddSip              String,
                            )
                                engine = MergeTree ORDER BY OperatorAddId;'''
            client.execute(sql_create_work)
        except (ValueError):
            print('Данные не загружены')
        finally:
            try:
                # Записываем новые данные в таблицу
                client.insert_dataframe('INSERT INTO suitecrm_robot_ch.usermetric_operatoradd VALUES', df_usertotal)
            except (ValueError):
                print('fyyst yt pfuhe;tys')
            finally:
                client.connection.disconnect()
                print('conection closed')


def userlogin_to_click(path_file, userlogin):

    # Выгружаем датафрейм с рабюочими часами
    df_userlogin = pd.read_csv(f'{path_file}{userlogin}')

    # Добавляем столбцы

    df_userlogin['date_start_status'] = df_userlogin['start_status'].astype('datetime64').dt.to_period('D')
    df_userlogin['date_stop_status'] = df_userlogin['stop_status'].astype('datetime64').dt.to_period('D')
    df_userlogin = df_userlogin.rename(columns={'user_id' : 'UserLoginUserId',
                                'start_status' : 'UserLoginStartStatus',
                                'stop_status' : 'UserLoginStopStatus',
                                'date_start_status' : 'UserLoginDateStart',
                                'date_stop_status' : 'UserLoginDateStop'})

    # # cохраним csv lля теста в даталенс
    # df_userlogin['UserLoginStartStatus'] = df_userlogin['UserLoginStartStatus'].astype('datetime64').dt.time
    # df_userlogin['UserLoginStopStatus'] = df_userlogin['UserLoginStopStatus'].astype('datetime64').dt.time
       

    # df_userlogin.to_csv('/root/airflow/dags/current_month_yesterday/Files/userlogin.csv')
    
    # print('save to csv') 

    # Подключаемся к базе
    print('connect to clickhouse')
    # Достаем host, user, password
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
        #                 database='suitecrm_robot_ch', settings={'use_numpy': True})
        client = my_connection()
        #  Очищаем данные из таблицы usermetric_userlogin
        print('delete dates from table')
        cluster = '{cluster}'
        client.execute(f'''truncate table usermetric_userlogin on cluster '{cluster}' ''') 
    except (ValueError):
        print('Данные не удалены')
    finally:
        try:

            # Создаем таблицу usermetric_userlogin
            print('create table')
            sql_create_userlogin = '''create table if not exists usermetric_userlogin
            
                                (   UserLoginUserId           String,
                                    UserLoginStartStatus      Time,
                                    UserLoginStopStatus       Time,
                                    UserLoginDateStart        Date,
                                    UserLoginDateStop         Date,    
                                )
                                engine = MergeTree ORDER BY UserLoginUserId;'''
            client.execute(sql_create_userlogin)
        except (ValueError):
            print('fyyst yt pfuhe;tys')
        finally:
            try:

                # Записываем новые данные в таблицу

                client.insert_dataframe('INSERT INTO suitecrm_robot_ch.usermetric_userlogin VALUES', df_userlogin)
            except (ValueError):
                print('Данные не удалены')
            finally:

                client.connection.disconnect()
                print('conection closed')

def user_to_click(path_file, user):

    # Выгружаем датафрейм с рабюочими часами
    df_user = pd.read_csv(f'{path_file}{user}')

    # Пренобразуем колонку team в строку
    df_user['team'] = df_user['team'].fillna(0).astype(str).apply(lambda x: x.replace('.0', ''))
    df_user['team']  = df_user['team'].apply(lambda x: x.replace('0', ''))
    df_user = df_user.fillna('')

    # Оставялем нужные колонки и перименовываем

    df_user = df_user[['id', 'fio', 'team', 'supervisor']].rename(columns = {'id' : 'OperatorId',
                    'fio' : 'OperatorFio',
                    'team' : 'OperatorTeam',
                    'supervisor' : 'OperatorSupervisor'})
    
    # Подключаемся к базе
    print('connect to clickhouse')

    # Достаем host, user, password
    # dest = '/root/airflow/dags/not_share/ClickHouse198.csv'
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
        client = my_connection()
    
        # Очищаем таблицу operator
        print('delete dates from table')
        cluster = '{cluster}'
        client.execute(f'''truncate table usermetric_operator on cluster '{cluster}' ''')
    except (ValueError):
            print('Таблица не удалена')
    finally:
        try:
            # Создаем таблицу operator

            print('create table')
            sql_create_user = '''create table if not exists usermetric_operator
            
                                (
                                OperatorId          String,
                                OperatorFio         String,
                                OperatorTeam        String,
                                OperatorSupervisor  String
                                )
                                engine = MergeTree ORDER BY OperatorId;'''

            client.execute(sql_create_user)
        except (ValueError):
            print('fyyst yt pfuhe;tys')
        finally:
            try:
                # Записываем новые данные в таблицу

                client.insert_dataframe('INSERT INTO suitecrm_robot_ch.usermetric_operator VALUES', df_user)
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

    df_union_user = df_union_user.fillna('')
    print('date since after merge ',df_union_user['dateCall'].min())
    print('size df ', df_union_user.shape[0])

    # Загружаем таблицу с лидами    
    df_lids = download_gs('Команды/Проекты', 'Лиды')

    # Загружаем таблицу с проектами 
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
    df_union_jc['otkaz_c'] = df_union_jc['otkaz_c'].fillna('0')
    df_union_jc = df_union_jc.fillna('')
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
    
    df_decoding_otkaz =  pd.read_excel('/root/airflow/dags/current_month_yesterday/Files/decoding.xlsx', sheet_name = 'Лист1')
    df_decoding_otkaz = df_decoding_otkaz.rename(columns={'name':'otkaz_c'})    
    
    df_decoding_otkaz['otkaz_c'] = df_decoding_otkaz['otkaz_c'].astype(str)
    
    df_union_otkaz =  df_union_jc.merge(df_decoding_otkaz, on = 'otkaz_c', how = 'left')
    df_union_otkaz['duration_minutes'] = df_union_otkaz['duration_minutes'].fillna('0')
    df_union_otkaz = df_union_otkaz.fillna('')

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
                                    'name_ru',
                                    'Область',
                                    'Город']].rename(columns={
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
                                                                'name_ru' : 'OtkazName',
                                                                'Область' : 'CallRegion',
                                                                'Город' : 'CallCity'
    
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
        # Очищаем таблицу usermetric_call

        print('delete dates from table userrefusal_call_current')
        cluster = '{cluster}'
        client.execute(f'''truncate table userrefusal_call_current on cluster '{cluster}' ''')

    except (ValueError):
        print('Таблица не удалена')
        
    finally:
        try:
      # Создаем таблицу usermetric_call
            print('Create table userrefusal_call_current')
            sql_create = '''create table if not exists userrefusal_call_current
                        (
                        CallDate            Date,
                        CallUserID          String,
                        CallQueue           String,            
                        CallResult          String,
                        CallCityCode        String,
                        CallCountId         Int64,
                        CallSetQueue        String,
                        CallDurationMinute  Int64,
                        CallMarker          String,
                        CallPTV             String,
                        UserFio             String,
                        UserSupervisor      String,
                        LidsProject         String,
                        JCRegion            String,
                        OtkazName           String,
                        CallRegion          String,
                        CallCity            String
                        )
                        engine = MergeTree ORDER BY CallDate;'''
            client.execute(sql_create)

        except (ValueError):
            print('fyyst yt pfuhe;tys')
        finally:
            try:

                # Записываем новый данные в таблицу userrefusal_call_current

                client.insert_dataframe('INSERT INTO suitecrm_robot_ch.userrefusal_call_current VALUES', df_union_city)
            except (ValueError):
                print('Данные не загружены')
            finally:

                client.connection.disconnect()
                print('connection closed')
   


def callwait_to_click(path_file, callwait):

    # Выгружаем датафрейм
    df_callwait = pd.read_csv(f'{path_file}{callwait}')

    # Удаляем '.0' в значениях

    df_callwait = df_callwait.fillna('').astype('str')
    col_list = ['town_c', 'last_queue_c']
    defs.del_point_zero(df_callwait, col_list)

    # Переименовываем столбцы

    df_callwait = df_callwait.rename(columns={
                                        'assigned_user_id' : 'CallWaitUserID',
                                        'dateentered' : 'CallWaitDateEntered',
                                        'contacts_status' : 'CalWaitContactStatus',
                                        'datestart' : 'CallWaitDateStart',
                                        'contact_id_c' : 'CallWaitContactID',
                                        'town_c' : 'CallWaitTownCode',
                                        'last_queue_c' : 'CallWaitLastQueue'})

    # Подключаемся к базе
    print('connect to clickhouse')

    # Достаем host, user, password
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
        client = my_connection()    
        # client = Client(host=host, port='9000', user=user, password=password,
        #             database='suitecrm_robot_ch', settings={'use_numpy': True})
    
        #  Очищаем таблицу usermetric_callwait
        print('delete from table')
        cluster = '{cluster}'
        client.execute(f'''truncate table usermetric_callwait on cluster '{cluster}' ''')
    except (ValueError):
        print('Данные не удалены')
    finally:
        try:
    
            # Создаем таблицу userlogin
            print('create table')
            sql_create = '''create table if not exists usermetric_callwait
                        (
                            CallWaitUserID        String,
                            CallWaitDateEntered   Date,
                            CalWaitContactStatus  String,
                            CallWaitDateStart     Date,
                            CallWaitContactID     String,
                            CallWaitTownCode      String,
                            CallWaitLastQueue     String
                            
                        )
                        engine = MergeTree ORDER BY CallWaitDateEntered;'''
            client.execute(sql_create)
        except (ValueError):
            print('fyyst yt pfuhe;tys')
        finally:
            try:

                client.insert_dataframe('INSERT INTO suitecrm_robot_ch.usermetric_callwait VALUES', df_callwait)   
            except (ValueError):
                print('Данные не загружены')
            finally:
                client.connection.disconnect()
                print('connection closed')
    



        
