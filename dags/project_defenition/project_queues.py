def project_queues():
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
    yesterday_date = current_date - datetime.timedelta(days=0)
    queues['date'] = yesterday_date.strftime('%Y-%m-%d')



    queues_paths = '/root/airflow/dags/project_defenition/projects/queues/queues_{}.csv'
    to_save = queues_paths.format(datetime.datetime.now().strftime("%Y_%m_%d"))
    queues.to_csv(to_save, index=False)