def project_steps():
    import pandas as pd
    import datetime
    import MySQLdb
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    host128 = "192.168.1.183"
    host = host128
    Con = MySQLdb.Connect(host=host, user="base_dep_slave", passwd="IyHBh9mDBdpg", db="suitecrm",
                        charset='utf8')
    current_date = datetime.datetime.now()
    yesterday_date = current_date - datetime.timedelta(days=1)
    sql_steps = '/root/airflow/dags/project_defenition/steps.sql'
    to_save = '/root/airflow/dags/project_defenition/projects/steps/steps_{}.csv'
    to_save_steps = to_save.format(yesterday_date.strftime("%Y_%m_%d"))

    print('Выгружаем данные steps')
    sql_steps = open(sql_steps, 'r')
    sql_steps = sql_steps.read()
    stepst = pd.read_sql_query(sql_steps, Con)
    

    path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json' 

    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
    gs = gspread.authorize(credentials)

    table_name4 = 'ЗАЯВКИ РТК'

    work_sheet4 = gs.open(table_name4)
    sheet4 = work_sheet4.worksheet('Очередь, шаг')
    data4 = sheet4.get_all_values() 
    headers4 = data4.pop(0) 
    shag = pd.DataFrame(data4, columns=headers4)
    shag['type_steps'] = '1'
    shag = shag.rename(columns={'Шаг': 'step',
                         'Диалог': 'ochered',})
    shag = shag[['step', 'ochered', 'type_steps']] 
    shag=shag.drop_duplicates().fillna('')


    steps= pd.concat([stepst, shag], ignore_index=True)


    current_date = datetime.datetime.now()
    yesterday_date = current_date - datetime.timedelta(days=1)
    steps['date'] = yesterday_date.strftime('%Y-%m-%d')
    steps = steps[['step','ochered','date','type_steps']]
    steps=steps.drop_duplicates().fillna('')

    steps.to_csv(to_save_steps, index=False)
