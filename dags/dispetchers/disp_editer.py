def disp_editors(path_to_files, lids, path_result):
    import pandas as pd
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import datetime
    import fsp.def_project_definition as def_project_definition



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
    queues['date'] = datetime.datetime.now().strftime('%Y-%m-%d')

    lid = pd.read_csv(f'{path_to_files}/{lids}')

    lid['Принимающая_очередь'] = lid['Принимающая_очередь'].astype('str').apply(lambda x: x.replace('.0',''))
    queues['Очередь'] = queues['Очередь'].astype('str').apply(lambda x: x.replace('.0',''))


    lid = lid.merge(queues, how='left', left_on='Принимающая_очередь', right_on='Очередь')
    lid['Последний_шаг'] = lid['Последний_шаг'].astype('str').apply(lambda x: x.replace('.0',''))
    lid.to_csv(f'{path_result}/{lids}', sep=',', index=False, encoding='utf-8')










    