def disp_editors(path_to_files, lids, path_result, calls):
    import pandas as pd
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import datetime
    import fsp.def_project_definition as def_project_definition
    from oauth2client.service_account import ServiceAccountCredentials
    import re
    import numpy



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


    calls = pd.read_csv(f'{path_to_files}/{calls}')
    calls = calls[calls['Результат'] != 'Назначена заявка']
    calls['Телефон']= calls['Телефон'].fillna(0).astype('int64')
    phones = calls.copy()
    phones = phones[['Телефон']]
    phones = phones.drop_duplicates()
    df = phones.merge(calls[['Проект','description','Ответственный перезвона',
        'ФИО','Команда','supervisor','Дата создания',
        'Дата и время звонка',
        'Статус','Очередь','Причина',
        'Город','Телефон']], left_on = 'Телефон', right_on = 'Телефон', how = 'left').fillna('')




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
    project = pd.DataFrame(data4, columns=headers4)


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

    df =  df.merge(project[['Проект','СВ CRM']], left_on = 'supervisor', right_on = 'СВ CRM', how = 'left').fillna('')
    df =  df.merge(jc[['Проект','CRM СВ']], left_on = 'supervisor', right_on = 'CRM СВ', how = 'left').fillna('')
    df = df.astype('str')

    def update_dom(row):
     if row['Проект_x'] == '' or row['Проект_x'] == ' ':
        if row['Проект_y'] != '' and row['Проект_y'] != ' ':
            row['Проект_x'] = row['Проект_y']
        elif row['Проект'] != '' and row['Проект'] != ' ':
            row['Проект_x'] = row['Проект']
        return row


    df.apply(lambda row: update_dom(row), axis=1)
    df = df[['Телефон','Проект_x','Ответственный перезвона'
         ,'ФИО','Команда','supervisor','Дата создания','Дата и время звонка',
         'Статус','Очередь','Причина','Город']]
    df = df[df['Команда'] != '12']
    df = df[df['Команда'] != '4']

    df = df[['Проект_x','Очередь','Причина','Город','Телефон']]
    
    df = df.drop_duplicates()
    df['Телефон']= df['Телефон'].fillna(0).astype('int64')
    t9293 = df[df['Проект_x'].isin(['TTK', 'TTK LIDS'])]
    t9295  = df[df['Проект_x'].isin(['MTS', 'MTS LIDS'])]
    t9296  = df[df['Проект_x'].isin(['BEELINE', 'BEELINE LIDS'])]
    t9297  = df[df['Проект_x'].isin(['RTK', 'RTK LIDS'])]
    t9298  = df[df['Проект_x'].isin(['NBN', 'NBN LIDS'])]
    t9299  = df[df['Проект_x'].isin(['DOMRU', 'DOMRU LIDS'])]
    t9052  = df[df['Проект_x'].isin(['TELE2', 'TELE2 LIDS'])]
    t9072  = df[df['Проект_x'].isin(['GULFSTREAM', 'GULFSTREAM LIDS'])]

    salary_sheets = {'9293': t9293, '9295': t9295,'9296': t9296, 
                 '9297': t9297,'9298': t9298, '9299 ': t9299,
                 '9052': t9052,'9072': t9072}
    writer = pd.ExcelWriter('/root/airflow/dags/dispetchers/Files/Перезвоны обработанный.xlsx', engine='xlsxwriter')
    for sheet_name in salary_sheets.keys():
        salary_sheets[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)

    writer.save()
    # writer.to_excel('/root/airflow/dags/dispetchers/Files/Перезвоны обработанный.xlsx', index=False)




#     writer = pd.ExcelWriter('/root/airflow/dags/dispetchers/Files/Перезвоны обработанный.xlsx', index=False)

# # Сохраняем каждый датафрейм на отдельном листе с использованием метода to_excel()
#     t9293.to_excel(writer, sheet_name='9293', index=False)
#     t9295.to_excel(writer, sheet_name='9295', index=False)
#     t9296.to_excel(writer, sheet_name='9296', index=False)
#     t9297.to_excel(writer, sheet_name='9297', index=False)
#     t9298.to_excel(writer, sheet_name='9298', index=False)
#     t9299.to_excel(writer, sheet_name='9299', index=False)
#     t9052.to_excel(writer, sheet_name='9052', index=False)
#     t9072.to_excel(writer, sheet_name='9072', index=False)
#     writer.save()

#     # writer.to_excel('/root/airflow/dags/dispetchers/Files/Перезвоны обработанный.xlsx', index=False)












    