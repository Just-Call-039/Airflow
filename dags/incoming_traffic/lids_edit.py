def lids_editing (waiters,lids,dop,path_to_file,file, path_file):
    import pandas as pd
    from datetime import timedelta
    from oauth2client.service_account import ServiceAccountCredentials
    from datetime import datetime, timedelta
    from datetime import datetime
    from clickhouse_driver import Client
    import pandas as pd
    import gspread


    
    waiters = pd.read_csv(f'{path_to_file}/{waiters}').fillna('')
    lids = pd.read_csv(f'{path_to_file}/{lids}').fillna('')
    dop = pd.read_csv(f'{path_to_file}/{dop}').fillna('')

    lids['type'] ='Лид'
    waiters['type'] ='Ждун'

    
    tab = pd.read_csv('/root/airflow/dags/incoming_traffic/Files/Лидовые шаги.csv',  sep=';', encoding='utf-8').fillna('')

    tab['Шаг'] = tab['Шаг'].astype('str').apply(lambda x: x.replace('.0',''))
    tab['Очередь'] = tab['Очередь'].astype('str').apply(lambda x: x.replace('.0',''))
    lids['dialog'] = lids['dialog'].astype('str').apply(lambda x: x.replace('.0',''))
    lids['last_step'] = lids['last_step'].astype('str').apply(lambda x: x.replace('.0',''))



    df = lids.merge(tab, left_on = ['dialog','last_step'], right_on = ['Очередь','Шаг'], how = 'inner').fillna('')
    

    waiters = waiters.astype('str')


    df = df[['phone','calldate','dialog','town_c','last_step','ptv','type']].astype('str')
    tt = pd.concat([waiters, df, dop])

    tt['phone'] = tt['phone'].astype('str').apply(lambda x: x.replace('.0',''))
    tt['dialog'] = tt['dialog'].astype('str').apply(lambda x: x.replace('.0',''))
    tt['last_step'] = tt['last_step'].astype('str').apply(lambda x: x.replace('.0',''))
    tt['town_c'] = tt['town_c'].astype('str').apply(lambda x: x.replace('.0',''))
    tt['last_step'] = tt['last_step'].astype('str').apply(lambda x: x.replace('.0',''))
    tt = tt.merge(tab, left_on = ['dialog','last_step'], right_on = ['Очередь','Шаг'], how = 'left').fillna('')
    tt = tt[['phone','calldate','dialog','town_c','last_step','ptv','type']].astype('str')
    tt= tt.drop_duplicates()

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

    tt = tt.merge(queues, how='left', left_on='dialog', right_on='Очередь')
    tt = tt[['phone','calldate','dialog','town_c','last_step','ptv','type','Группировка']].astype('str')

    salary_sheets = {'Лист1': tt}
    writer = pd.ExcelWriter(rf'{path_file}/{file}', engine='xlsxwriter')
    for sheet_name in salary_sheets.keys():
        salary_sheets[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)

    writer.save()

    # tt.to_csv(rf'{path_file}/{file}', index=False, sep=',', encoding='utf-8')
