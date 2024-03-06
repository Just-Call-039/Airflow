def region_editer(path_to_files, requests, path_result, file_result_req,file_result):
 import pandas as pd
 import pymysql
 import gspread
 from oauth2client.service_account import ServiceAccountCredentials
 import datetime
 import os
 import glob
 import indicators_to_regions.defs as defs


 print('Начинаем обработку данных')
 requests = pd.read_csv(f'{path_to_files}/{requests}')
 print('Читаем звонки из папки')

 csv_files = glob.glob('/root/airflow/dags/indicators_to_regions/Files/sql_files/callls/*.csv')
 dataframes = []

 for file in csv_files:
    df = pd.read_csv(file)
    dataframes.append(df)

 calls = pd.concat(dataframes)

 calls.reset_index(drop=True, inplace=True)
 print('Читаем переводы из папки')

 csv_files = glob.glob('/root/airflow/dags/indicators_to_regions/Files/transfer/*.csv')
 dataframes = []

 for file in csv_files:
    df = pd.read_csv(file)
    dataframes.append(df)

 transferfull = pd.concat(dataframes)

 transferfull.reset_index(drop=True, inplace=True)

 calls['datecall'] = calls['datecall'].astype(str)
 calls['hoursonly'] = calls['hoursonly'].astype(str)
 calls['hoursonly'] = calls['hoursonly'].apply(lambda x: x.replace('.0', ''))
 calls['phone'] = calls['phone'].astype(str)
 calls['phone'] = calls['phone'].apply(lambda x: x.replace('.0', ''))
 calls['queue_c'] = calls['queue_c'].astype(str)
 calls['queue_c'] = calls['queue_c'].apply(lambda x: x.replace('.0', ''))


 transferfull['datecalls'] = transferfull['datecalls'].astype(str)    
 transferfull['hoursonly'] = transferfull['hoursonly'].astype(str)
 transferfull['hoursonly'] = transferfull['hoursonly'].apply(lambda x: x.replace('.0', ''))
 transferfull['phone'] = transferfull['phone'].astype(str)
 transferfull['phone'] = transferfull['phone'].apply(lambda x: x.replace('.0', ''))
 transferfull['dialog'] = transferfull['dialog'].astype(str)
 transferfull['dialog'] = transferfull['dialog'].apply(lambda x: x.replace('.0', ''))
 transferfull['destination_queue'] = transferfull['destination_queue'].astype(str)
 transferfull['destination_queue'] = transferfull['destination_queue'].apply(lambda x: x.replace('.0', ''))
 transferfull['city_c'] = transferfull['city_c'].astype(str)
 transferfull['city_c'] = transferfull['city_c'].apply(lambda x: x.replace('.0', ''))
 transferfull['town'] = transferfull['town'].astype(str)
 transferfull['town'] = transferfull['town'].apply(lambda x: x.replace('.0', ''))
 requests['contact'] = requests['contact'].astype(str)
 requests['contact'] = requests['contact'].apply(lambda x: x.replace('.0', ''))
 print(transferfull.head(10))
 print(calls.head(10))
 print('Переводы полностью прочитаны')
 calls.loc[calls['phone'] == '89922404564']
 transferfull.loc[transferfull['phone'] == '89922404564']
 calltransfer = calls.merge(transferfull, how='left', left_on=['phone', 'datecall', 'hoursonly'],
                           right_on=['phone', 'datecalls', 'hoursonly'])
 print(calltransfer.head(10))
 calltransfer.loc[calltransfer['phone'] == '89922404564']
 calltransfer['phone'] = calltransfer['phone'].astype(str)
 calltransfer['phone'] = calltransfer['phone'].apply(lambda x: x.replace('.0', ''))

 requests = requests.merge(calltransfer, how='left', left_on=['contact', 'userid'], right_on=['phone', 'userid'])
 print(requests.head(10))
 requests.loc[requests['contact'] == '89922404564']

 print('Заявки с переводами соединены')
 path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json'

 scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

 credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
 gs = gspread.authorize(credentials)

 table_name4 = 'Команды/Проекты'

 work_sheet4 = gs.open(table_name4)
 sheet4 = work_sheet4.worksheet('Регионы-Города')
 data4 = sheet4.get_all_values()
 headers4 = data4.pop(0)
 city1 = pd.DataFrame(data4, columns=headers4)

 city2 = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv', sep=',', encoding='utf-8')

 city3 = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv', sep=',', encoding='utf-8')

 city4 = city3.drop_duplicates(subset='town_c', keep='first', inplace=False)

 city4 = city4.drop(['city_c', 'Город', 'MACRO'], axis=1)
 city2['city_c'] = city2['city_c'].astype(str)
 city4['town_c'] = city4['town_c'].astype(str)
 print('Все файлы для соединения полностью прочитаны')

 requests['city_c'] = requests['city_c'].astype(str)
 requests['city_c'] = requests['city_c'].apply(lambda x: x.replace('.0', ''))
 requests['town'] = requests['town'].astype(str)
 requests['town'] = requests['town'].apply(lambda x: x.replace('.0', ''))
 requestcitys = requests.merge(city1, how='left', left_on='city', right_on='Город')

 requestcitys1 = requestcitys.merge(city2, how='left', left_on='city_c', right_on='city_c')

 requestcitys2 = requestcitys1.merge(city4, how='left', left_on='town', right_on='town_c')

 requestcitys2.drop(
    ['contact', 'datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'MACRO', 'Город_x', 'Город_y', 'queue_c', 'hoursonly',
     'project_c'], axis=1, inplace=True)

 requestcitys2.rename(
    columns={'Регион': 'RTK_city', 'Регион ТТК': 'TTK_city', 'Регионы МТС': 'MTS_city', 'Регион Билайн': 'BLN_city',
             'ТТК Регион_x': 'TTK_city_c', 'РТК Регион_x': 'RTK_city_c', 'МТС Регион_x': 'MTS_city_c',
             'Билайн Регион_x': 'BLN_city_c', 'ТТК Регион_y': 'TTK_town', 'РТК Регион_y': 'RTK_town',
             'Билайн Регион_y': 'BLN_town', 'МТС Регион_y': 'MTS_town'}, inplace=True)

 requestcitys3 = requestcitys2.fillna('')
 requestcitys3['RTK_region'] = requestcitys3.apply(lambda row: defs.rtk_reg_r(row), axis=1)
 requestcitys3['TTK_region'] = requestcitys3.apply(lambda row: defs.ttk_reg_r(row), axis=1)
 requestcitys3['MTS_region'] = requestcitys3.apply(lambda row: defs.mts_reg_r(row), axis=1)
 requestcitys3['BLN_region'] = requestcitys3.apply(lambda row: defs.bln_reg_r(row), axis=1)
 requestcitys3.drop(
    ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
     'RTK_town', 'MTS_town', 'BLN_town'], axis=1, inplace=True)

 requestcitys3.fillna('').to_csv(f'{path_result}/{file_result_req}', index=False,
                                sep=',',
                                encoding='utf-8')
 print('Заявки записаны в файл')
 calltransfer['city_c'] = calltransfer['city_c'].astype(str)
 calltransfer['city_c'] = calltransfer['city_c'].apply(lambda x: x.replace('.0', ''))
 calltransfer['town'] = calltransfer['town'].astype(str)
 calltransfer['town'] = calltransfer['town'].apply(lambda x: x.replace('.0', ''))
 callscitys = calltransfer.merge(city1, how='left', left_on='city', right_on='Город')
 callscitys1 = callscitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
 callscitys2 = callscitys1.merge(city4, how='left', left_on='town', right_on='town_c')
 callscitys2.drop(['datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'MACRO', 'Город_x', 'Город_y', 'queue_c'], axis=1,
                 inplace=True)

 callscitys2.rename(
    columns={'Регион': 'RTK_city', 'Регион ТТК': 'TTK_city', 'Регионы МТС': 'MTS_city', 'Регион Билайн': 'BLN_city',
             'ТТК Регион_x': 'TTK_city_c', 'РТК Регион_x': 'RTK_city_c', 'МТС Регион_x': 'MTS_city_c',
             'Билайн Регион_x': 'BLN_city_c', 'ТТК Регион_y': 'TTK_town', 'РТК Регион_y': 'RTK_town',
             'Билайн Регион_y': 'BLN_town', 'МТС Регион_y': 'MTS_town'}, inplace=True)

 callscitys2 = callscitys2.fillna('')
 callscitys2['RTK_region'] = callscitys2.apply(lambda row: defs.rtk_reg(row), axis=1)
 print('РТК регион')

 callscitys2['TTK_region'] = callscitys2.apply(lambda row: defs.ttk_reg(row), axis=1)
 print('ТТК регион')
 
 callscitys2['MTS_region'] = callscitys2.apply(lambda row: defs.mts_reg(row), axis=1)
 print('МТС регион')

 callscitys2['BLN_region'] = callscitys2.apply(lambda row: defs.bln_reg(row), axis=1)
 print('Билайн регион')
 callscitys2.drop(
    ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
     'RTK_town', 'MTS_town', 'BLN_town'], axis=1, inplace=True)

 callscitys2 = callscitys2.drop_duplicates().fillna('')
 print('Записывается в файл')

 callscitys2.to_csv(f'{path_result}/{file_result}',sep=',', index=False)

