def project_queues(path_to_folder, file_name):
    import pandas as pd
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import datetime


    path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json' 
    table_name = 'Команды/Проекты'

    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
    gs = gspread.authorize(credentials)

    work_sheet = gs.open(table_name)
    table_name4 = 'Группировка очередей'


    work_sheet4 = gs.open(table_name4)
    sheet4 = work_sheet4.worksheet('Лист1')
    data4 = sheet4.get_all_values() 
    headers4 = data4.pop(0) 
    queues = pd.DataFrame(data4, columns=headers4)
    current_date = datetime.datetime.now()
    queues['date'] = current_date.strftime('%Y-%m-%d')

    queues.to_csv(f'{path_to_folder}{file_name}', index=False)

def project_teams(path_to_folder, file_name):
    import pandas as pd
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import datetime


    path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json' 
    table_name = 'Команды/Проекты'

    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
    gs = gspread.authorize(credentials)

    work_sheet = gs.open(table_name)
    sheet1 = work_sheet.worksheet('JC') 
    data1 = sheet1.get_all_values() 
    headers1 = data1.pop(0) 

    sheet2 = work_sheet.worksheet('Лиды')
    data2 = sheet2.get_all_values() 
    headers2 = data2.pop(0) 

    sheet3 = work_sheet.worksheet('Вход, ОД')
    data3 = sheet3.get_all_values() 
    headers3 = data3.pop(0) 


    teams_jc = pd.DataFrame(data1, columns=headers1)
    teams_lids = pd.DataFrame(data2, columns=headers2)
    teams_od = pd.DataFrame(data3, columns=headers3)
    teams_lids = teams_lids.rename(columns={'Номер команды': '№ команды'})
    teams_lids = teams_lids.rename(columns={'СВ CRM': 'CRM СВ'})
    teams_jc = teams_jc.rename(columns={'Номер команды': '№ команды'})
    teams_jc = teams_jc.rename(columns={'СВ CRM': 'CRM СВ'})


    df = teams_jc[['№ команды','Проект','CRM СВ']].append(teams_od[['№ команды','Проект','CRM СВ']])
    df = df.append(teams_lids[['№ команды','Проект','CRM СВ']])

    


    def project_correct(row):
        if row == 'РТК Lids':
            return 'RTK LIDS'
        elif row == 'MTC Lids':
            return 'MTS LIDS'
        elif row == 'Domru Lids':
            return 'DOMRU LIDS'
        elif row == 'ТТК Lids':
            return 'TTK LIDS'
        elif row == 'МТС':
            return 'MTS'
        elif row == 'NBN Lids':
            return 'NBN LIDS'
        elif row == 'РТК':
            return 'RTK'
        elif row == 'Мегафон':
            return 'NBN'
        elif row == 'Beeline Lids':
            return 'BEELINE LIDS'
        elif row == 'РТК Лиды':
            return 'RTK LIDS'
        else:
            return row

    сurrent_date = datetime.datetime.now()
    
    df['project'] = df['Проект'].apply(lambda row: project_correct(row))
    df = df[['№ команды','project','CRM СВ']].rename(columns={'№ команды': 'team', 'CRM СВ': 'supervisor'})
    df['date'] = сurrent_date.strftime('%Y-%m-%d')
    df['team'] = df['team'].astype('str').apply(lambda x: x.replace('я',''))
    df['team'] = df['team'].astype('str').apply(lambda x: x.replace('.0',''))
    df['team'] = df['team'].astype('str').apply(lambda x: x.replace('nan',''))

    df.to_csv(f'{path_to_folder}{file_name}', index=False)
