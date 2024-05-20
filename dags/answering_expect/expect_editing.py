def edit_ex(path_file, expect, total):
    import pandas as pd
    import pymysql
    import gspread 
    from oauth2client.service_account import ServiceAccountCredentials
    import datetime
    from datetime import datetime, timedelta
    from datetime import datetime
    from clickhouse_driver import Client
    import pandas as pd
    import pymysql
    import datetime
    from clickhouse_driver import Client
    import datetime


    print('Читаем созданный файл')
    df_robot = pd.read_csv(f'{path_file}/{expect}')
    df_r= df_robot
    df_r[['dialog']]=df_r[['dialog']].astype('int64')

    df_r = df_r['dialog'].drop_duplicates()

    df_r = df_r.tolist()
    df_r.sort()
    import time
    start_time = time.time()
    print(f'Начало скрипта в: {time.strftime("%X")}.')

    user='robot_read_only'
    password='du9Itg5bnzTb'

    servers = ['192.168.1.15',
# '192.168.1.131',
'192.168.1.81',
'192.168.1.36',
'192.168.1.84',
'192.168.1.85',
# '192.168.1.123',
'192.168.1.103',
'192.168.1.86',
'192.168.1.122',
'192.168.1.124',
'192.168.1.125',
'192.168.1.127',
'192.168.1.129',
'192.168.1.126',
'192.168.1.130',
'192.168.1.87',
'192.168.1.59',
'192.168.1.114',
'192.168.1.146',
'192.168.1.109',
'192.168.1.107',
'192.168.1.156',
'192.168.1.110',
'192.168.1.113',
'192.168.1.118',
'192.168.1.116',
'192.168.1.147']


    no_data_rollback = pd.DataFrame()
    df = pd.DataFrame()
    queue_list = df_r
    k = 0

    df_full = pd.DataFrame()
    for queue in queue_list:
        for server in servers:
            try:
                connection = pymysql.connect(host= server,
                                 user=user,
                                 password=password,
                                 database='robot',
                                 cursorclass=pymysql.cursors.DictCursor)
                with connection.cursor() as cursor:
                    try:
                        print({server},{queue})
                        sql = f'''select distinct phone,date,step_start,step_end,normalized,full_normalized,`set`,
                `match`,result,{queue} as dialog, type, a.uniqueid uid
                 from `cel_just-call-{queue}-new_1`  a
                 left join `bill_cdr` b  on a.uniqueid = b.uniqueid
                 where 
                 date(date) = date(now())-interval 1 day and type = 'expect'
                 '''
                        cursor.execute(sql)
                        result = cursor.fetchone()
                        df = pd.read_sql_query(sql, connection)
    
                        df_full = df_full.append(df)
                    except pymysql.err.ProgrammingError as e:
                        print(f"Error: {e} Table cel_just-call-{queue}-new_1 not found.")
                        continue
                        k += 1
                
            except pymysql.err.ProgrammingError as e:
                print(f"Error: {e} Table cel_just-call-{queue}-new_1 not found.")
                continue
                k += 1
        
    df_full['date'] = pd.to_datetime(df_full['date'])        
    df_full2 = df_full
    start_time = time.time()
    print(f'Конец: {time.strftime("%X")}.')
    df_full2=df_full2.drop_duplicates().fillna('')
    print('Проставляем ранг по экспектам')
    df_full2['Rank'] = df_full2.groupby(['phone','dialog'])['date'].rank(ascending=True).fillna('0')
    df_full2['Rank'] = df_full2['Rank'].astype('str').apply(lambda x: x.replace('.0',''))
    df_full2['full_normalized'] = df_full2['Rank'] + '. '+ df_full2['full_normalized']
    df_full2 = df_full2[['phone','step_start','step_end','full_normalized','dialog','uid','Rank']]
    df_robot= df_robot.astype('str')
    df_full2= df_full2.astype('str')
    print('Соединяем роботлог с экспектами')
    tab =  df_robot.merge(df_full2, left_on = ['phone','dialog'], right_on = ['phone','dialog'], how = 'left').fillna('')

    tab['trunk_id'] = tab['trunk_id'].astype('str').apply(lambda x: x.replace('.0',''))
    tab['step_end'] = tab['step_end'].astype('str').apply(lambda x: x.replace('.0',''))
    tab['trunk_id'] = tab['trunk_id'].astype('str').apply(lambda x: x.replace('.0',''))
    tab['call_sec'] = tab['call_sec'].fillna(0).astype('int64')
    print('Группируем и делим длительность на количество строк')
    tab['new_callsec'] =tab.groupby(['phone','dialog','last_step'])['call_sec'].transform(lambda x: x / len(x))
    tab = tab[['dialog',
           'calldate',
           'autootvet',
           'talk',
           'last_step',
           'trunk_id',
           'marker',
           'uniqueid',
           'ptv_c',
           'city_c',
           'call_sec',
           'new_callsec',
           'otkaz',
           'calls',
           'step_start',
          'step_end',
          'full_normalized',
          'Rank']] 
    tab['new_callsec'] = tab['new_callsec'].fillna(0).astype('int64')
    # print('Записывается в файл')
    # tab.to_csv(f'{path_file}/{total}',sep=',', index=False)

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

    tab[['dialog','last_step','trunk_id','marker','uniqueid',
        'ptv_c','city_c','step_start','step_end','full_normalized','Rank']] =  tab[['dialog','last_step','trunk_id','marker','uniqueid',
        'ptv_c','city_c','step_start','step_end','full_normalized','Rank']].fillna('').astype('str')
    
    tab['ptv_c'] = tab['ptv_c'].astype('str').apply(lambda x: x.replace('nan',''))
    tab['trunk_id'] = tab['trunk_id'].astype('str').apply(lambda x: x.replace('nan',''))
    tab['full_normalized'] = tab['full_normalized'].astype('str').apply(lambda x: x.replace('nan',''))
    tab['step_start'] = tab['step_start'].astype('str').apply(lambda x: x.replace('nan',''))
    tab['step_end'] = tab['step_end'].astype('str').apply(lambda x: x.replace('nan',''))

        
        
    tab[['autootvet','talk','otkaz','calls',
            'call_sec','new_callsec']] =  tab[['autootvet','talk','otkaz','calls',
            'call_sec','new_callsec']].fillna(0).astype('int64')

        
        
    print('Загрузка в базу')
    tab['calldate'] = pd.to_datetime(tab['calldate'])


    tab = tab[['dialog','calldate','autootvet','talk','last_step','trunk_id','marker','uniqueid',
        'ptv_c','city_c','call_sec','new_callsec','otkaz','calls','step_start','step_end','full_normalized','Rank']]


    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})
        
    print('Отправляем запрос')
    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.expect_voicemail VALUES', tab)

