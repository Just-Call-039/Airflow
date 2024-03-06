def traffic_in_editing (calls,robot,plancall,ishod,trafic, path_file, path_to_file, calls_file, traffic_file, lids_file):
    import pandas as pd
    import pymysql
    import gspread 
    from oauth2client.service_account import ServiceAccountCredentials
    from datetime import datetime, timedelta
    import os
    import glob
    from datetime import datetime
    from clickhouse_driver import Client
    import datetime
    

    calls = pd.read_csv(f'{path_file}/{calls}').fillna('')
    robot = pd.read_csv(f'{path_file}/{robot}').fillna('')
    plancall = pd.read_csv(f'{path_file}/{plancall}').fillna('')
    ishod = pd.read_csv(f'{path_file}/{ishod}').fillna('')
    trafic = pd.read_csv(f'{path_file}/{trafic}').fillna('')
    csv_files = glob.glob('/root/airflow/dags/project_defenition/projects/steps/*.csv')
    dataframes  = []

    for file in csv_files:
        df = pd.read_csv(file)
        dataframes.append(df)

    steps = pd.concat(dataframes)
    steps['step'] = steps['step'].astype('str').apply(lambda x: x.replace('.0',''))
    steps['ochered'] = steps['ochered'].astype('str').apply(lambda x: x.replace('.0',''))
    steps['type_steps'] = steps['type_steps'].astype('str').apply(lambda x: x.replace('.0',''))
    csv_files = glob.glob('/root/airflow/dags/project_defenition/projects/teams/*.csv')
    dataframes  = []

    for file in csv_files:
        df = pd.read_csv(file)
        dataframes.append(df)

    teams = pd.concat(dataframes)
    teams['team'] = teams['team'].astype('str').apply(lambda x: x.replace('.0',''))
    teams['date'] = pd.to_datetime(teams['date'])

    users = pd.read_csv('/root/airflow/dags/incoming_line/Files/sql_calls/users.csv',  sep=',', encoding='utf-8').fillna('')
    users['team'] = users['team'].astype('str').apply(lambda x: x.replace('.0',''))

    csv_files = glob.glob('/root/airflow/dags/project_defenition/projects/queues/*.csv')
    dataframes  = []

    for file in csv_files:
        df = pd.read_csv(file)
        dataframes.append(df)

    queues = pd.concat(dataframes)
    queues['Очередь'] = queues['Очередь'].astype('str').apply(lambda x: x.replace('.0',''))
    queues['date'] = pd.to_datetime(queues['date'])



    steps = steps[steps['type_steps'] == '0']

    robot['dialog'] = robot['dialog'].astype('str').apply(lambda x: x.replace('.0',''))
    robot['last_step'] = robot['last_step'].astype('str').apply(lambda x: x.replace('.0',''))
    steps['date'] = pd.to_datetime(steps['date'])
    robot['calldate'] = pd.to_datetime(robot['calldate'])


    tab1 = robot.merge(steps, left_on = ['calldate','dialog','last_step'], right_on = ['date','ochered','step'], how = 'inner').fillna('')
    tab1['types'] ='lids' 
    tab1['phone'] = tab1['phone'].astype('str').apply(lambda x: x.replace('.0',''))


    plancall['types1'] ='planned' 
    ishod['types2'] ='outcall' 
    plancall = plancall.merge(users, left_on = 'usersid', right_on = 'id', how = 'left').fillna('')
    plancall['dateentered'] = pd.to_datetime(plancall['dateentered'])
    plancall = plancall.merge(teams, left_on = ['team','dateentered'], right_on = ['team','date'], how = 'left').fillna('')


    ishod = ishod.merge(users, left_on = 'assigned_user_id', right_on = 'id', how = 'left').fillna('')
    ishod['calldate'] = pd.to_datetime(ishod['calldate'])
    ishod = ishod.merge(teams, left_on = ['team','calldate'], right_on = ['team','date'], how = 'left').fillna('')





    csv_files = glob.glob('/root/airflow/dags/project_defenition/projects/steps/*.csv')
    dataframes  = []

    for file in csv_files:
        df = pd.read_csv(file)
        dataframes.append(df)

    steps = pd.concat(dataframes)
    steps['step'] = steps['step'].astype('str').apply(lambda x: x.replace('.0',''))
    steps['ochered'] = steps['ochered'].astype('str').apply(lambda x: x.replace('.0',''))
    steps['type_steps'] = steps['type_steps'].astype('str').apply(lambda x: x.replace('.0',''))
    trafic['dialog'] = trafic['dialog'].astype('str').apply(lambda x: x.replace('.0',''))
    trafic['last_step'] = trafic['last_step'].astype('str').apply(lambda x: x.replace('.0',''))

    steps['date'] = pd.to_datetime(steps['date'])
    trafic['calldate'] = trafic['calldate'].astype('str').apply(lambda x: x.replace('.0',''))

    trafic['calldate'] = pd.to_datetime(trafic['calldate'])
    trafic['perevod'] = ''
    trafic['perevelis'] = ''
    trafic1 = trafic.merge(steps, left_on = ['calldate','dialog','last_step'], right_on = ['date','ochered','step'], how = 'left').fillna('')
    print('Цепим переводы')
       
    def check_1(row):
        if row['type_steps'] == '1':
            return '1'
        else:
            return '0'
    trafic1['perevod'] = trafic1.apply(check_1, axis=1)

    print('Цепим лиды')

    def check_2(row):
        if (row['type_steps'] == '1') & (row['client_status'] in ['refusing','MeetingWait','CallWait']) :
            return '1'
        else:
            return '0'
    trafic1['perevelis'] = trafic1.apply(check_2, axis=1)

    trafic1['phone'] = trafic1['phone'].astype('str').apply(lambda x: x.replace('.0',''))
    tab1['phone'] = tab1['phone'].astype('str').apply(lambda x: x.replace('.0',''))
    plancall['phone'] = plancall['phone'].astype('str').apply(lambda x: x.replace('.0',''))
    ishod['phone'] = ishod['phone'].astype('str').apply(lambda x: x.replace('.0',''))

    trafic2 = trafic1.merge(tab1, left_on = 'phone', right_on = 'phone', how = 'left').fillna('')
    trafic2 = trafic2.merge(plancall, left_on = 'phone', right_on = 'phone', how = 'left').fillna('')
    trafic2 = trafic2.merge(ishod, left_on = 'phone', right_on = 'phone', how = 'left').fillna('')


    trafic2['date_type'] = ''
    trafic2['queue_type'] = ''
    trafic2['type'] = ''
    trafic2['type'] = trafic2['type'].astype('str')
    trafic2['calldate_y'] = pd.to_datetime(trafic2['calldate_y'])
    trafic2['dateentered'] = pd.to_datetime(trafic2['dateentered'])
    trafic2['calldate'] = pd.to_datetime(trafic2['calldate'])


    trafic2['calldate_y'] = trafic2['calldate_y'].astype('str').apply(lambda x: x.replace('NaT','2023-01-01'))
    trafic2['dateentered'] = trafic2['dateentered'].astype('str').apply(lambda x: x.replace('NaT','2023-01-01'))
    trafic2['calldate'] = trafic2['calldate'].astype('str').apply(lambda x: x.replace('NaT','2023-01-01'))
    trafic2['queue_type'] = trafic2['queue_type'].astype('str')
    trafic2 = trafic2.astype(str)


    def dates(row):
        if (row['calldate_y'] == '2023-01-01') & (row['dateentered'] == '2023-01-01') & (row['calldate'] == '2023-01-01'):
            row['date_type'] = ''
            row['queue_type'] = ''
            row['type'] = ''
        elif (row['calldate_y'] > row['dateentered']) & (row['calldate_y'] > row['calldate']):
            row['date_type'] = row['calldate_y']
            row['queue_type'] = row['dialog_y']
            row['type'] = row['types']
        elif (row['dateentered'] > row['calldate_y']) & (row['dateentered'] > row['calldate']):
            row['date_type'] = row['dateentered']
            row['queue_type'] = row['last_queue_c']
            row['type'] = row['types1']     
        elif (row['calldate'] > row['calldate_y']) & (row['calldate'] > row['dateentered']):
            row['date_type'] = row['calldate']
            row['queue_type'] = row['queue_c']
            row['type'] = row['types2'] 
        
    trafic2.apply(lambda row: dates(row), axis=1)



    trafic2=trafic2[['calldate_x','phone',
           'dialog_x','assigned_user_id_x',
           'last_step_x','client_status',
           'real_billsec','town_c_x', 'city_c_x',
           'perevod','perevelis',
           'date_type','queue_type',
           'type']].rename(columns={'calldate_x': 'calldate','dialog_x': 'dialog',
                                   'assigned_user_id_x': 'userid','last_step_x': 'last_step',
                                    'town_c_x': 'town_c','city_c_x': 'city_c'
                                   })
    

    calls_dispetcher = calls.copy()
    calls_dispetcher = calls_dispetcher.astype('str')
    calls_dispetcher['date_type'] = ''
    calls_dispetcher['queue_type'] = ''
    calls_dispetcher['type'] = ''
    def type_call(row):
        if (row['direction'] == 'I') & (row['result_call_c'] == 'MeetingWait') & (row['type'] == ''):
            row['date_type'] = row['calldate']
            row['queue_type'] = row['dialog_one']
            row['type'] = 'meeting'
        elif (row['direction'] == 'I') & (row['result_call_c'] != 'MeetingWait') & (row['type'] == ''):
            row['date_type'] = row['calldate']
            row['queue_type'] = row['dialog_one']
            row['type'] = 'inbound'

    calls_dispetcher.apply(lambda row: type_call(row), axis=1)  
    tab1.to_csv(rf'{path_to_file}/{lids_file}', index=False, sep=',', encoding='utf-8')
    trafic2.to_csv(rf'{path_to_file}/{traffic_file}', index=False, sep=',', encoding='utf-8')
    calls_dispetcher.to_csv(rf'{path_to_file}/{calls_file}', index=False, sep=',', encoding='utf-8')