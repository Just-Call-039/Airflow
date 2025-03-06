def etv_universal():
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
    from commons_liza import to_click


    path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json' 

    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
    gs = gspread.authorize(credentials)

    table_name4 = 'Группировка очередей'

    work_sheet4 = gs.open(table_name4)
    sheet4 = work_sheet4.worksheet('Лист1')
    data4 = sheet4.get_all_values() 
    headers4 = data4.pop(0) 
    step = pd.DataFrame(data4, columns=headers4)


    #  tab = df.merge(step, left_on = 'dialog', right_on = 'Очередь', how = 'left').fillna('')
    step = step[step['Группировка'].str.contains('УНИВЕРСАЛ', case=False)]
    step[['Очередь']]=step[['Очередь']].astype('int64')
    step = step['Очередь']
    step = step.tolist()



    df = pd.DataFrame()
    queue_list = step
    print('queue_list', queue_list)

    k = 0

    for queue in queue_list:
        print(f' Очередь {queue}, Выгрузка {k}')
        sql = f'''select distinct jc_robot_log.phone,
                         date(call_date)                       calldate,
                         jc_robot_log.assigned_user_id,
                         substring(jc_robot_log.dialog, 11, 4) dialog,
                         client_status,
                         last_step,
                         was_repeat,
                         if(contacts_cstm.city_c is null or contacts_cstm.city_c = '' or contacts_cstm.city_c = 0,
                            concat(town_c, '_t'),
                            contacts_cstm.city_c)              city_c,
                         if(house is null, 0, 1)               etv,
                         providers
         from suitecrm_robot.jc_robot_log
                  left join address_log al on jc_robot_log.dialog = al.dialog and al.phone = jc_robot_log.phone AND
                                              date(calldate) = date(call_date)
                  left join suitecrm.contacts
                            on jc_robot_log.phone = phone_work
                  left join suitecrm.contacts_cstm on contacts.id = id_c
         where last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
           and date(call_date) = date(now()) - interval 1 day
           and (inbound_call = 0 or inbound_call = '')
             and substring(jc_robot_log.dialog,11,4) = {queue}
    '''

        Con = pymysql.Connect(host="192.168.1.183", user="base_dep_slave", passwd="IyHBh9mDBdpg", db="suitecrm",
                      charset='utf8')
    
    
        robot = pd.read_sql_query(sql, Con)
        print('robot size', robot.shape[0])
        df = df.append(robot)
        print('df size', df.shape[0])
        k += 1  

    if df.shape[0] > 0:

        df['providers'] = df['providers'].str.split(',')
        df = df.explode('providers')
        df = df.fillna('').astype('str')


        path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json' 

        scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

        credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
        gs = gspread.authorize(credentials)

        table_name4 = 'Группировка очередей'

        work_sheet4 = gs.open(table_name4)
        sheet4 = work_sheet4.worksheet('Лист1')
        data4 = sheet4.get_all_values() 
        headers4 = data4.pop(0) 
        step = pd.DataFrame(data4, columns=headers4)


        tab = df.merge(step, left_on = 'dialog', right_on = 'Очередь', how = 'left').fillna('')
        tab = tab[tab['Группировка'].str.contains('УНИВЕРСАЛ', case=False)]


        tab=tab.astype('str')
        tab['providers2']=''

        def update_providers(row):
            if row['providers'] == '3':
                row['providers2'] = 'DOMRU'
            elif row['providers'] == '5':
                row['providers2'] = 'RTK'
            elif row['providers'] == '6':
                row['providers2'] = 'TTK'
            elif row['providers'] == '10':
                row['providers2'] = 'BEELINE'
            elif row['providers'] == '11':
                row['providers2'] = 'MTS'
            elif row['providers'] == '19':
                row['providers2'] = 'NBN'
            elif row['providers'] == '14':
                row['providers2'] = '2COM'  
            elif row['providers'] == '211':
                row['providers2'] = 'TAT'
            else:
                row['providers2'] = ''
            
            
        tab.apply(lambda row: update_providers(row), axis=1)
        print(tab.shape[0])
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
        users = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/users.csv',  sep=',', encoding='utf-8').astype('str').fillna('')

        print(tab.shape[0])
        tab = tab.merge(users[['id','supervisor']], left_on = 'assigned_user_id', right_on = 'id', how = 'left').fillna('')
        tab =  tab.merge(lids[['Проект','СВ CRM']], left_on = 'supervisor', right_on = 'СВ CRM', how = 'left').fillna('')
        tab =  tab.merge(jc[['Проект','CRM СВ']], left_on = 'supervisor', right_on = 'CRM СВ', how = 'left').fillna('')
        tab = tab.astype('str')
        def update_project(row):
            if row['Проект_x'] == '':
                row['Проект_x'] = row['Проект_y']
            else:
                row['Проект_x']
        tab.apply(lambda row: update_project(row), axis=1)
        tab['phone']=tab['phone'].astype('int64')
        print(tab.shape[0])


        tab = tab[['calldate',
            'assigned_user_id',
            'dialog',
            'client_status',
            'last_step',
            'was_repeat',
            'city_c','etv',
            'providers','phone',
            'Проект (набирающая очередь)',
            'providers2','Проект_x']].rename(columns={'providers2': 'project_address',
                            'Проект (набирающая очередь)': 'project_log',
                            'Проект_x': 'project_users'})
        

        csv_files = glob.glob('/root/airflow/dags/project_defenition/projects/steps/*.csv')
        dataframes  = []

        for file in csv_files:
            df = pd.read_csv(file)
            dataframes.append(df)

        steps = pd.concat(dataframes)
        steps['step'] = steps['step'].astype('str').apply(lambda x: x.replace('.0',''))
        steps['ochered'] = steps['ochered'].astype('str').apply(lambda x: x.replace('.0',''))
        steps['type_steps'] = steps['type_steps'].astype('str').apply(lambda x: x.replace('.0',''))

        tab['perevod'] = ''
        tab = tab.merge(steps, left_on = ['calldate','dialog','last_step'], right_on = ['date','ochered','step'], how = 'left').fillna('')
        print('Цепим переводы')
        
        def check_1(row):
            if row['type_steps'] == '1':
                return '1'
            else:
                return '0'
            
        print(tab.shape[0])
            
        tab['perevod'] = tab.apply(check_1, axis=1)

        tab=tab[['calldate',
            'assigned_user_id',
            'dialog',
            'client_status',
            'last_step',
            'was_repeat',
            'city_c','etv',
            'providers','phone','project_log','project_address','project_users','perevod']]
        
        tab[['calldate','assigned_user_id','dialog','client_status','last_step',
        'was_repeat','city_c','etv','providers','project_log','project_address',
        'project_users','perevod']]=tab[['calldate','assigned_user_id','dialog','client_status','last_step',
        'was_repeat','city_c','etv','providers','project_log','project_address',
        'project_users','perevod']].astype('str')
        
        tab[['phone']] = tab[['phone']].astype('int64')
        
        print('Подключаемся к clickhouse')
        
        client = to_click.my_connection()
        print(tab.shape[0])
            
        print('Отправляем запрос')
        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.etv_universal VALUES', tab)
        
    else:
        pass

