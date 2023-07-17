def project_teams():
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
    sheet1 = work_sheet.sheet1 
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
            'DR'


    df['project'] = df['Проект'].apply(lambda row: project_correct(row))
    df = df[['№ команды','project','CRM СВ']].rename(columns={'№ команды': 'team', 'CRM СВ': 'supervisor'})
    df['date'] = datetime.datetime.now().strftime('%Y-%m-%d')


    team_paths = '/root/airflow/dags/project_defenition/projects/teams/teams_{}.csv'
    to_save = team_paths.format(datetime.datetime.now().strftime("%Y_%m_%d"))
    df.to_csv(to_save, index=False)


    def project_team(x):
        for i in df['project'].unique():
            x = x.replace(' ', '').replace(' ','')
            if x in df[df['project'] == i]['team'].to_list():
                return i