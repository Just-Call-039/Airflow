def to_click(path_file, calls):
    import pandas as pd   
    import datetime
    from datetime import datetime
    from clickhouse_driver import Client
    from indicators_to_regions.download_googlesheet import download_gs

    df = pd.read_csv(f'{path_file}/{calls}')
    print('самая рання дата ', df['call_date'].min())

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
    print('самая ранняя дата после мерджа с пользователями ', df['call_date'].min())
    print('размер датасета  ', df.shape[0])
    users = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/users.csv',  sep=',', encoding='utf-8').fillna('')
    df = df.merge(users, left_on = 'user_call', right_on = 'id', how = 'left').fillna('')
    print('самая ранняя дата после мерджа с пользователями ', df['call_date'].min())
    print('размер датасета  ', df.shape[0])
    
    
    lids = download_gs('Команды/Проекты', 'Лиды')
    jc = download_gs('Команды/Проекты', 'JC')
    
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
    print('даты после мерджа с заявками', df1['call_date'].unique())
    print('размер датасета  ', df1.shape[0])
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


    import datetime
    today = datetime.date.today()
    current_month = today.strftime('%Y-%m')

# Получаем начальную и конечную даты текущего месяца
    first_day = current_month + '-01'
    last_day = today.strftime('%Y-%m-%d')

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
    try:
        client = Client(host=host, port='9000', user=user, password=password,
                        database='suitecrm_robot_ch', settings={'use_numpy': True})

        print('Удаляем таблицу')
        client.execute('truncate table suitecrm_robot_ch.pokazateli_operatorov')
    except (ValueError):
        print('Таблица не удалена')
    finally:
        try:

            print('Создаем таблицу')
            sql_create = '''create table if not exists suitecrm_robot_ch.pokazateli_operatorov
        (
            id            String,
            call_date     Date,
            name          String,
            contactid     String,
            queue         String,
            user_call     String,
            super         String,
            city          String,
            town          String,
            call_sec      Int64,
            short_calls   Int64,
            dialog        String,
            completed_c   String,
            fio           String,
            supervisor    String,
            project       String,
            call_count    Float64,
            request_date  Date,
            user          String,
            status        String,
            district_c    String,
            my_phone_work String
        )
            engine = MergeTree ORDER BY call_date;'''
            client.execute(sql_create)
        except (ValueError):
            print('Таблица не удалена')
        finally:
            try:
                client.insert_dataframe('INSERT INTO suitecrm_robot_ch.pokazateli_operatorov VALUES', df1)
            except (ValueError):
                print('Таблица не удалена')
            finally:

                client.connection.disconnect()
                print('conection closed')

