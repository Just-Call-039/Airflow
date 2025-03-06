import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import glob
import indicators_to_regions.defs as defs
from indicators_to_regions import download_googlesheet

def region_editer(path_to_files, requests, path_result, file_result_req, file_result):

 print('start proccess')
 requests = pd.read_csv(f'{path_to_files}/{requests}', sep = ',')
 print('read call')

 csv_files = glob.glob('/root/airflow/dags/indicators_to_regions/Files/sql_files/callls/*.csv')
 dataframes = []

 for file in csv_files:
    df = pd.read_csv(file)
    dataframes.append(df)

 calls = pd.concat(dataframes)
 print('concat call succesfull')

 calls.reset_index(drop=True, inplace=True)
 print('read transfer')
 print('columns calls ', calls.columns)

 csv_files = glob.glob('/root/airflow/dags/indicators_to_regions/Files/transfer/*.csv')
 dataframes = []

 for file in csv_files:
    df = pd.read_csv(file)
    dataframes.append(df)

 transferfull = pd.concat(dataframes)
 print('__concat transfer succesfull')
 print('columns transfer ', transferfull.columns)

 transferfull = transferfull.drop_duplicates(subset=['phone', 'datecalls'], keep='last')
 print('__delete duplicates')
 transferfull.reset_index(drop=True, inplace=True)
 print('size transfer: ', transferfull.shape[0])
 print('size call: ', calls.shape[0])

 print('start change types data')

 calls['datecall'] = calls['datecall'].astype(str)
 calls['hoursonly'] = calls['hoursonly'].astype(str).apply(lambda x: x.replace('.0', ''))
 calls['phone'] = calls['phone'].astype(str).apply(lambda x: x.replace('.0', ''))
 calls['queue_c'] = calls['queue_c'].fillna(0).astype(int).astype(str)
 calls['city_c'] = calls['city_c'].fillna(0).astype(int).astype(str)
 
 transferfull['datecalls'] = transferfull['datecalls'].astype(str)    
 transferfull['hoursonly'] = transferfull['hoursonly'].astype(str).apply(lambda x: x.replace('.0', ''))
 transferfull['phone'] =transferfull['phone'].astype(str).apply(lambda x: x.replace('.0', ''))
 transferfull['dialog'] = transferfull['dialog'].fillna(0).astype(str)
 transferfull['destination_queue'] = transferfull['destination_queue'].fillna(0).astype(str)
 transferfull['city_c'] = transferfull['city_c'].fillna(0).astype(int).astype(str)
 transferfull['town'] = transferfull['town'].fillna(0).astype(int).astype(str)
 
 print(requests.columns)

 requests['contact'] = requests['contact'].astype(str).apply(lambda x: x.replace('.0', ''))
 requests['queue'] = requests['queue'].astype(str).apply(lambda x: x.replace('.0', ''))


 print('datatypes changed')

 calltransfer = calls.merge(transferfull, how='left', left_on=['phone', 'datecall', 'hoursonly'],
                           right_on=['phone', 'datecalls', 'hoursonly'])
 
# Мердж на случай если будут вылезать дубли 
#  calltransfer = calls.merge(transferfull, how='left', left_on=['phone', 'datecall', 'hoursonly', 'queue_c'],
#                            right_on=['phone', 'datecalls', 'hoursonly', 'destination_queue'])
 print('merge calls and transfer finish')
 print('size new df ', calltransfer.shape[0])

 calltransfer['city_c_x'] = calltransfer['city_c_x'].fillna('0').astype('int64')
 calltransfer['city_c_y'] = calltransfer['city_c_y'].fillna('0').astype('int64')

 calltransfer['city_c'] = calltransfer.apply(lambda row: defs.area_defination(row['city_c_x'], row['city_c_y']), axis=1)

 del calltransfer['city_c_x']
 del calltransfer['city_c_y']

 print('defination city_c complete')
 
 calltransfer['town'] = calltransfer['town'].fillna('0').astype('int64')
 
 calltransfer['city'] = calltransfer['city'].fillna('0')

 calltransfer['city_guess'] = calltransfer.sort_values(['datecall', 'phone', 'city_c'], ascending=False).\
                                          groupby(['datecall', 'phone'])['city_c'].cummax()
 calltransfer['town_guess'] = calltransfer.sort_values(['datecall', 'phone', 'town'], ascending=False).\
                                          groupby(['datecall', 'phone'])['town'].cummax()
 print('create columns guess town and city')

 calltransfer['city_c'] = calltransfer.apply(lambda row: defs.area_defination(row['city_c'], row['city_guess']), axis = 1).astype(str)
 calltransfer['town'] = calltransfer.apply(lambda row: defs.area_defination(row['town'], row['town_guess']), axis = 1).astype(str)
 print('city_c and town was definated')

 call_noduplicates = calltransfer.sort_values(['datecall', 'phone', 'city_c', 'town']).drop_duplicates(subset=['phone', 'userid'], keep='last')

 print('start merge with request')
 print('size_request: ', requests.shape[0])
 

 requests = requests.merge(call_noduplicates, how='left', left_on=['contact', 'userid'], right_on=['phone', 'userid'])

 print('merge with request finished')
 print('size new df: ', requests.shape[0])
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
 print('size city1 ', city1.shape[0])
 city1 = city1.drop_duplicates(subset='Город', keep='first', inplace=False)
 print('size city1 after drop duplicates', city1.shape[0])

 city2 = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv', sep=',', encoding='utf-8')
 del city2['Название из JSON карты']

 

 city2['Город'] = city2['Город'].apply(defs.find_letter)
 city2 = city2[['city_c', 'town_c', 'Город', 'Область', 'ТТК Регион', 'РТК Регион', 'МТС Регион', 'Билайн Регион']].merge(
                            city1, how = 'left', on = 'Город')
 
 city2 = city2.fillna('0')
 city2['ТТК Регион'] = city2.apply(lambda row: defs.area_defination_str(row['ТТК Регион'], row['Регион ТТК']), axis = 1)
 city2['РТК Регион'] = city2.apply(lambda row: defs.area_defination_str(row['РТК Регион'], row['Регион']), axis = 1)
 city2['МТС Регион'] = city2.apply(lambda row: defs.area_defination_str(row['МТС Регион'], row['Регионы МТС']), axis = 1)
 city2['Билайн Регион'] = city2.apply(lambda row: defs.area_defination_str(row['Билайн Регион'], row['Регион Билайн']), axis = 1)
 
 del city2['Регион ТТК']
 del city2['Регион']
 del city2['Регионы МТС']
 del city2['Регион Билайн']

 city1 = city1.merge(city2[['Город', 'ТТК Регион', 'РТК Регион', 'МТС Регион', 'Билайн Регион']].drop_duplicates('Город'), how = 'left', on = 'Город')
 city1 = city1.fillna('0')
 city1['Регион ТТК'] = city1.apply(lambda row: defs.area_defination_str(row['Регион ТТК'], row['ТТК Регион']), axis = 1)
 city1['Регион'] = city1.apply(lambda row: defs.area_defination_str(row['Регион'], row['РТК Регион']), axis = 1)
 city1['Регионы МТС'] = city1.apply(lambda row: defs.area_defination_str(row['Регионы МТС'], row['МТС Регион']), axis = 1)
 city1['Регион Билайн'] = city1.apply(lambda row: defs.area_defination_str(row['Регион Билайн'], row['Билайн Регион']), axis = 1)

 del city1['ТТК Регион']
 del city1['РТК Регион']
 del city1['МТС Регион']
 del city1['Билайн Регион']

 city4 = city2.drop_duplicates(subset='town_c', keep='first', inplace=False)

 city4 = city4.drop(['city_c', 'Город'], axis=1)
 city2['city_c'] = city2['city_c'].astype(str)
 city4['town_c'] = city4['town_c'].astype(str)
 print('Все файлы для соединения полностью прочитаны')
 print('requests: col city_c type before changed ', requests['city_c'].dtype, 'col town type', requests['town'].dtype)

 requests['city_c'] = requests['city_c'].astype(str).apply(lambda x: x.replace('.0', ''))
 requests['town'] = requests['town'].astype(str).apply(lambda x: x.replace('.0', ''))
 requests['queue'] = requests['queue'].astype(str).apply(lambda x: x.replace('.0', ''))
 
 print('reqauest: col city_c type after changed ', requests['city_c'].dtype, 'col town type', requests['town'].dtype)
 print('city1: col city_c type', city1['Город'].dtype)
 print('city2: col town type', city2['city_c'].dtype)
 print('city4: col town_c type', city4['town_c'].dtype)
 print('count of city not null in request: ', requests[requests['city'] != '0']['city'].count())

 requestcitys = requests.merge(city1, how='left', left_on='city', right_on='Город')
 print('merge with city1 finished')
 print('size new df: ', requestcitys.shape[0])
 requestcitys['Регион'] = requestcitys['Регион'].fillna('0')
 
 print('count of Город: ', requestcitys[requestcitys['Регион'] != '0']['Регион'].count())
 

 requestcitys1 = requestcitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
 print('merge with city2 finished')
 print('size new df: ', requestcitys1.shape[0])
 requestcitys1['Город_y'] = requestcitys1['Город_y'].fillna('0')
 print('count of Город: ', requestcitys1[requestcitys1['Город_y'] != '0'].count())

 requestcitys2 = requestcitys1.merge(city4, how='left', left_on='town', right_on='town_c')
 print('merge with city4 finished')
 print('size new df: ', requestcitys2.shape[0])
 requestcitys2['Область_y'] = requestcitys2['Область_y'].fillna('0')
 print('count of Город: ', requestcitys2[requestcitys2['Область_y'] != '0'].count())

 requestcitys2.drop(
    ['contact', 'datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'Город_x', 'Город_y', 'queue_c', 'hoursonly',
     'project_c'], axis=1, inplace=True)

 requestcitys2.rename(
    columns={'Регион': 'RTK_city',
              'Регион ТТК': 'TTK_city',
              'Регионы МТС': 'MTS_city',
              'Регион Билайн': 'BLN_city',
              'ТТК Регион_x': 'TTK_city_c',
              'РТК Регион_x': 'RTK_city_c',
              'МТС Регион_x': 'MTS_city_c',
              'Билайн Регион_x': 'BLN_city_c',
              'ТТК Регион_y': 'TTK_town',
              'РТК Регион_y': 'RTK_town',
              'Билайн Регион_y': 'BLN_town',
              'МТС Регион_y': 'MTS_town'}, inplace=True)
 
 col_list = ['userid',
            'statused',
            'queue', 
            'regions',
            'result_call_c',
            'otkaz_c', 
            'phone', 
            'duration_minutes',
            'city', 
            'town', 
            'dialog', 
            'destination_queue', 
            'city_c', 
            'Область_y', 
            'RTK_city',
            'TTK_city',
            'MTS_city',
            'BLN_city',
            'TTK_city_c',
            'RTK_city_c',
            'MTS_city_c',
            'BLN_city_c',
            'TTK_town',
            'RTK_town',
            'BLN_town',
            'MTS_town']
 
 for col in col_list:
   requestcitys2[col] = requestcitys2[col].fillna('0')

 requestcitys2['dateentered'] = requestcitys2['dateentered'].fillna('')
 requestcitys2['datecall'] = requestcitys2['datecall'].fillna('')

 requestcitys2['RTK_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['RTK_city'], 
                                                                                       row['RTK_city_c'],
                                                                                       row['RTK_town']), axis=1) 
 requestcitys2['TTK_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['TTK_city'], 
                                                                                       row['TTK_city_c'],
                                                                                       row['TTK_town']), axis=1) 
 requestcitys2['MTS_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['MTS_city'], 
                                                                                       row['MTS_city_c'],
                                                                                       row['MTS_town']), axis=1) 
 requestcitys2['BLN_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['BLN_city'], 
                                                                                       row['BLN_city_c'],
                                                                                       row['BLN_town']), axis=1) 
 
 requestcitys2.drop(
    ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
     'RTK_town', 'MTS_town', 'BLN_town', 'city_guess', 'town_guess'], axis=1, inplace=True)
 
 request_old = pd.read_csv(f'{path_result}{file_request_result}')

 request_old['NN'] = request_old.sort_values(['dateentered', 'userid', 'phone', 'queue']).\
                groupby(['userid', 'dateentered', 'phone', 'queue']).cumcount() + 1
 requestcitys2['NN'] = requestcitys2.sort_values(['dateentered', 'userid', 'phone', 'queue']).\
                groupby(['userid', 'dateentered', 'phone', 'queue']).cumcount() + 1

 request_old['city_c'] = request_old['city_c'].astype(str).apply(lambda x: x.replace('.0', ''))
 request_old['town'] = request_old['town'].astype(str).apply(lambda x: x.replace('.0', ''))
 request_old['queue'] = request_old['queue'].astype(str).apply(lambda x: x.replace('.0', ''))

 request = request_old.merge(requestcitys2[requestcitys2['dateentered'] < str(date_i)][['userid', 'dateentered', 'phone', 'statused', 'queue', 'NN']],
                    how = 'left', on = ['userid', 'dateentered', 'queue', 'phone', 'NN'])

 request['statused_y'] = request['statused_y'].fillna('0')

 def get_status(y, x):
    
    if y == '0':
        return x
    else:
        return y

 request['statused'] = request.apply(lambda row: get_status(row['statused_y'], row['statused_x']), axis = 1)

 request = pd.concat([request[request['dateentered'] < str(date_i)][['userid', 'dateentered', 'phone', 'queue', 'regions', 'datecall', 'result_call_c', 'otkaz_c', 'duration_minutes',
                            'city', 'town', 'dialog', 'destination_queue', 'city_c', 'Область_y', 'RTK_region', 'TTK_region', 'MTS_region',
                            'BLN_region']], requestcitys2[requestcitys2['dateentered'] >= str(date_i)]]).reset_index()

 requestcitys2.to_csv(f'{path_result}/{file_result_req}', index=False,
                                sep=',',
                                encoding='utf-8')

 print('size request df: ', requestcitys2.shape[0])
 print('save request to csv')

 calltransfer['city_c'] = calltransfer['city_c'].astype(str)
 calltransfer['city_c'] = calltransfer['city_c'].apply(lambda x: x.replace('.0', ''))
 calltransfer['town'] = calltransfer['town'].astype(str)
 calltransfer['town'] = calltransfer['town'].apply(lambda x: x.replace('.0', ''))

 print('calltransfer: col city_c type', calltransfer['city_c'].dtype, 'col town type', calltransfer['town'].dtype)
 print('city1: col city_c type', city1['Город'].dtype)
 print('city2: col town type', city2['city_c'].dtype)
 print('city4: col town_c type', city4['town_c'].dtype)

 callscitys = calltransfer.merge(city1, how='left', left_on='city', right_on='Город')
 print('merge with city1 finished')
 print('size new df: ', callscitys.shape[0])
 callscitys1 = callscitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
 print('merge with city2 finished')
 print('size new df: ', callscitys1.shape[0])
 callscitys2 = callscitys1.merge(city4, how='left', left_on='town', right_on='town_c')
 print('merge with city4 finished')
 print('size new df: ', callscitys2.shape[0])
 print('count calls 9287 dialog', callscitys2[callscitys2['datecall'] > '2024-09-01'].groupby('dialog')['phone'].count()['9287'])
 print('count calls 9278 dialog', callscitys2[callscitys2['datecall'] > '2024-09-01'].groupby('dialog')['phone'].count()['9278'])

 callscitys2.drop(['datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'Город_x', 'Город_y', 'queue_c'], axis=1,
                 inplace=True)

 callscitys2.rename(
    columns={'Регион': 'RTK_city', 'Регион ТТК': 'TTK_city', 'Регионы МТС': 'MTS_city', 'Регион Билайн': 'BLN_city',
             'ТТК Регион_x': 'TTK_city_c', 'РТК Регион_x': 'RTK_city_c', 'МТС Регион_x': 'MTS_city_c',
             'Билайн Регион_x': 'BLN_city_c', 'ТТК Регион_y': 'TTK_town', 'РТК Регион_y': 'RTK_town',
             'Билайн Регион_y': 'BLN_town', 'МТС Регион_y': 'MTS_town'}, inplace=True)

 callscitys2 = callscitys2.fillna('0')
 callscitys2['RTK_region'] = callscitys2.apply(lambda row: defs.region_defination(row['RTK_city'], 
                                                                                 row['RTK_city_c'],
                                                                                 row['RTK_town']), axis=1)
 print('РТК регион')
 callscitys2['TTK_region'] = callscitys2.apply(lambda row: defs.region_defination(row['TTK_city'], 
                                                                                 row['TTK_city_c'],
                                                                                 row['TTK_town']), axis=1) 
 print('ТТК регион')
 callscitys2['MTS_region'] = callscitys2.apply(lambda row: defs.region_defination(row['MTS_city'], 
                                                                                 row['MTS_city_c'],
                                                                                 row['MTS_town']), axis=1) 
 print('МТС регион')
 callscitys2['BLN_region'] = callscitys2.apply(lambda row: defs.region_defination(row['BLN_city'], 
                                                                                 row['BLN_city_c'],
                                                                                 row['BLN_town']), axis=1) 

 callscitys2.drop(
    ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
     'RTK_town', 'MTS_town', 'BLN_town', 'city_guess', 'town_guess'], axis=1, inplace=True)

 callscitys2 = callscitys2.drop_duplicates().fillna('')
 
 callscitys2.to_csv(f'{path_result}/{file_result}',sep=',', index=False)


def region_editer_per_month(path_to_file, file_request, path_to_request, file_request_prev,
                           path_to_file_sql, file_call,
                           path_to_sql_transfer, csv_transfer,
                           path_result, file_request_result, file_call_result, i_date):
 
 print(i_date)

 print('start proccess')
 requests = pd.read_csv(f'{path_result}{file_request}', sep=';')

 print('read call')

 calls = pd.read_csv(f'{path_to_file_sql}{file_call}', sep=';')
 print('concat call succesfull')

 calls.reset_index(drop=True, inplace=True)
 print('read transfer')

 transferfull = pd.read_csv(f'{path_to_sql_transfer}{csv_transfer}', sep=';')
 print('__concat transfer succesfull')
 
 transferfull = transferfull.drop_duplicates(subset=['phone', 'datecalls'], keep='last')
 print('__delete duplicates')
 transferfull.reset_index(drop=True, inplace=True)
 print('size transfer: ', transferfull.shape[0])
 print('size call: ', calls.shape[0])

 print('start change types data')

 calls['datecall'] = calls['datecall'].astype(str)
 calls['hoursonly'] = calls['hoursonly'].astype(str).apply(lambda x: x.replace('.0', ''))
 calls['phone'] = calls['phone'].astype(str).apply(lambda x: x.replace('.0', ''))
 calls['queue_c'] = calls['queue_c'].fillna(0).astype(int).astype(str)
 calls['city_c'] = calls['city_c'].fillna(0).astype(int).astype(str)
 
 transferfull['datecalls'] = transferfull['datecalls'].astype(str)    
 transferfull['hoursonly'] = transferfull['hoursonly'].astype(str).apply(lambda x: x.replace('.0', ''))
 transferfull['phone'] =transferfull['phone'].astype(str).apply(lambda x: x.replace('.0', ''))
 transferfull['dialog'] = transferfull['dialog'].fillna(0).astype(str)
 transferfull['destination_queue'] = transferfull['destination_queue'].fillna(0).astype(str)
 transferfull['city_c'] = transferfull['city_c'].fillna(0).astype(int).astype(str)
 transferfull['town'] = transferfull['town'].fillna(0).astype(int).astype(str)
 

 requests['contact'] = requests['contact'].astype(str).apply(lambda x: x.replace('.0', ''))
 requests['queue'] = requests['queue'].astype(str).apply(lambda x: x.replace('.0', ''))

 print('datatypes changed')

 calltransfer = calls.merge(transferfull, how='left', left_on=['phone', 'datecall', 'hoursonly'],
                           right_on=['phone', 'datecalls', 'hoursonly'])
 
# Мердж на случай если будут вылезать дубли 
#  calltransfer = calls.merge(transferfull, how='left', left_on=['phone', 'datecall', 'hoursonly', 'queue_c'],
#                            right_on=['phone', 'datecalls', 'hoursonly', 'destination_queue'])
 print('merge calls and transfer finish')
 print('size new df ', calltransfer.shape[0])

 calltransfer['city_c_x'] = calltransfer['city_c_x'].fillna('0').astype('int64')
 calltransfer['city_c_y'] = calltransfer['city_c_y'].fillna('0').astype('int64')

 calltransfer['city_c'] = calltransfer.apply(lambda row: defs.area_defination(row['city_c_x'], row['city_c_y']), axis=1)

 del calltransfer['city_c_x']
 del calltransfer['city_c_y']

 print('defination city_c complete')
 
 calltransfer['town'] = calltransfer['town'].fillna('0').astype('int64')
 
 calltransfer['city'] = calltransfer['city'].fillna('0')

 calltransfer['city_guess'] = calltransfer.sort_values(['datecall', 'phone', 'city_c'], ascending=False).\
                                          groupby(['datecall', 'phone'])['city_c'].cummax()
 calltransfer['town_guess'] = calltransfer.sort_values(['datecall', 'phone', 'town'], ascending=False).\
                                          groupby(['datecall', 'phone'])['town'].cummax()
 print('create columns guess town and city')

 calltransfer['city_c'] = calltransfer.apply(lambda row: defs.area_defination(row['city_c'], row['city_guess']), axis = 1).astype(str)
 calltransfer['town'] = calltransfer.apply(lambda row: defs.area_defination(row['town'], row['town_guess']), axis = 1).astype(str)
 print('city_c and town was definated')

 call_noduplicates = calltransfer.sort_values(['datecall', 'phone', 'city_c', 'town']).drop_duplicates(subset=['phone', 'userid'], keep='last')

 print('start merge with request')
 print('size_request: ', requests.shape[0])
 
 requests = requests.merge(call_noduplicates, how='left', left_on=['contact', 'userid'], right_on=['phone', 'userid'])
 
 print('merge with request finished')
 print('size new df: ', requests.shape[0])

 # Загружаем датасеты-справочники с городами
 
 city1 = download_googlesheet.download_gs('Команды/Проекты', 'Регионы-Города')
 print('size city1 ', city1.shape[0])
 city1 = city1.drop_duplicates(subset='Город', keep='first', inplace=False)
 print('size city1 after drop duplicates', city1.shape[0])

 city2 = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv', sep=',', encoding='utf-8')
 del city2['Название из JSON карты']

 city2['Город'] = city2['Город'].apply(defs.find_letter)
 city2 = city2[['city_c', 'town_c', 'Город', 'Область', 'ТТК Регион', 'РТК Регион', 'МТС Регион', 'Билайн Регион']].merge(
                            city1, how = 'left', on = 'Город')
 
 city2 = city2.fillna('0')
 city2['ТТК Регион'] = city2.apply(lambda row: defs.area_defination_str(row['ТТК Регион'], row['Регион ТТК']), axis = 1)
 city2['РТК Регион'] = city2.apply(lambda row: defs.area_defination_str(row['РТК Регион'], row['Регион']), axis = 1)
 city2['МТС Регион'] = city2.apply(lambda row: defs.area_defination_str(row['МТС Регион'], row['Регионы МТС']), axis = 1)
 city2['Билайн Регион'] = city2.apply(lambda row: defs.area_defination_str(row['Билайн Регион'], row['Регион Билайн']), axis = 1)
 
 del city2['Регион ТТК']
 del city2['Регион']
 del city2['Регионы МТС']
 del city2['Регион Билайн']

 city1 = city1.merge(city2[['Город', 'ТТК Регион', 'РТК Регион', 'МТС Регион', 'Билайн Регион']].drop_duplicates('Город'), how = 'left', on = 'Город')
 city1 = city1.fillna('0')
 city1['Регион ТТК'] = city1.apply(lambda row: defs.area_defination_str(row['Регион ТТК'], row['ТТК Регион']), axis = 1)
 city1['Регион'] = city1.apply(lambda row: defs.area_defination_str(row['Регион'], row['РТК Регион']), axis = 1)
 city1['Регионы МТС'] = city1.apply(lambda row: defs.area_defination_str(row['Регионы МТС'], row['МТС Регион']), axis = 1)
 city1['Регион Билайн'] = city1.apply(lambda row: defs.area_defination_str(row['Регион Билайн'], row['Билайн Регион']), axis = 1)

 del city1['ТТК Регион']
 del city1['РТК Регион']
 del city1['МТС Регион']
 del city1['Билайн Регион']

 city4 = city2.drop_duplicates(subset='town_c', keep='first', inplace=False)

 city4 = city4.drop(['city_c', 'Город'], axis=1)
 city2['city_c'] = city2['city_c'].astype(str)
 city4['town_c'] = city4['town_c'].astype(str)
 print('Все файлы для соединения полностью прочитаны')

 requests['city_c'] = requests['city_c'].astype(str).apply(lambda x: x.replace('.0', ''))
 requests['town'] = requests['town'].astype(str).apply(lambda x: x.replace('.0', ''))
 requests['queue'] = requests['queue'].astype(str).apply(lambda x: x.replace('.0', ''))

 requestcitys = requests.merge(city1, how='left', left_on='city', right_on='Город')
 print('merge with city1 finished')
 print('size new df: ', requestcitys.shape[0])
 requestcitys['Регион'] = requestcitys['Регион'].fillna('0')
 
 print('count of Город: ', requestcitys[requestcitys['Регион'] != '0']['Регион'].count())
 
 requestcitys1 = requestcitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
 print('merge with city2 finished')
 print('size new df: ', requestcitys1.shape[0])
 requestcitys1['Город_y'] = requestcitys1['Город_y'].fillna('0')
 print('count of Город: ', requestcitys1[requestcitys1['Город_y'] != '0']['Город_y'].count())

 requestcitys2 = requestcitys1.merge(city4, how='left', left_on='town', right_on='town_c')
 
 print('merge with city4 finished')
 print('size new df: ', requestcitys2.shape[0])
 requestcitys2['Область_y'] = requestcitys2['Область_y'].fillna('0')
 print('count of Город: ', requestcitys2[requestcitys2['Область_y'] != '0']['Область_y'].count())

 requestcitys2.drop(
    ['phone', 'datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'Город_x', 'Город_y', 'queue_c', 'hoursonly',
     'project_c'], axis=1, inplace=True)

 requestcitys2.rename(
    columns={'Регион': 'RTK_city',
              'Регион ТТК': 'TTK_city',
              'Регионы МТС': 'MTS_city',
              'Регион Билайн': 'BLN_city',
              'ТТК Регион_x': 'TTK_city_c',
              'РТК Регион_x': 'RTK_city_c',
              'МТС Регион_x': 'MTS_city_c',
              'Билайн Регион_x': 'BLN_city_c',
              'ТТК Регион_y': 'TTK_town',
              'РТК Регион_y': 'RTK_town',
              'Билайн Регион_y': 'BLN_town',
              'МТС Регион_y': 'MTS_town', 
              'contact' : 'phone'}, inplace=True)
 
 col_list = ['userid',
            'statused',
            'queue', 
            'regions',
            'result_call_c',
            'otkaz_c', 
            'phone', 
            'duration_minutes',
            'city', 
            'town', 
            'dialog', 
            'destination_queue', 
            'city_c', 
            'Область_y', 
            'RTK_city',
            'TTK_city',
            'MTS_city',
            'BLN_city',
            'TTK_city_c',
            'RTK_city_c',
            'MTS_city_c',
            'BLN_city_c',
            'TTK_town',
            'RTK_town',
            'BLN_town',
            'MTS_town']
 
 for col in col_list:
   requestcitys2[col] = requestcitys2[col].fillna('0')

 requestcitys2['dateentered'] = requestcitys2['dateentered'].fillna('')
 requestcitys2['datecall'] = requestcitys2['datecall'].fillna('')

 requestcitys2['RTK_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['RTK_city'], 
                                                                                       row['RTK_city_c'],
                                                                                       row['RTK_town']), axis=1) 
 requestcitys2['TTK_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['TTK_city'], 
                                                                                       row['TTK_city_c'],
                                                                                       row['TTK_town']), axis=1) 
 requestcitys2['MTS_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['MTS_city'], 
                                                                                       row['MTS_city_c'],
                                                                                       row['MTS_town']), axis=1) 
 requestcitys2['BLN_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['BLN_city'], 
                                                                                       row['BLN_city_c'],
                                                                                       row['BLN_town']), axis=1) 
 
 requestcitys2.drop(
    ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
     'RTK_town', 'MTS_town', 'BLN_town', 'city_guess', 'town_guess'], axis=1, inplace=True)

 request_old = pd.read_csv(f'{path_to_request}{file_request_prev}')
 print(i_date)

 request_old['NN'] = request_old.sort_values(['dateentered', 'userid', 'phone', 'queue']).\
                    groupby(['userid', 'dateentered', 'phone', 'queue']).cumcount() + 1
 requestcitys2['NN'] = requestcitys2.sort_values(['dateentered', 'userid', 'phone', 'queue']).\
                    groupby(['userid', 'dateentered', 'phone', 'queue']).cumcount() + 1
 
 request_old['city_c'] = request_old['city_c'].astype(str).apply(lambda x: x.replace('.0', ''))
 request_old['town'] = request_old['town'].astype(str).apply(lambda x: x.replace('.0', ''))
 request_old['queue'] = request_old['queue'].astype(str).apply(lambda x: x.replace('.0', ''))
 
 request = request_old.merge(requestcitys2[requestcitys2['dateentered'] < str(i_date)][['userid', 'dateentered', 'phone', 'statused', 'queue', 'NN']],
                       how = 'left', on = ['userid', 'dateentered', 'queue', 'phone', 'NN'])
 
 request['statused_y'] = request['statused_y'].fillna('0')
 print(request[(request['statused_y'] == 'Held') | (request['statused_y'] == 'Active')].groupby('statused_y')[['dateentered']].count())
 print(request[(request['statused_x'] == 'Held') | (request['statused_x'] == 'Active')].groupby('statused_x')[['dateentered']].count())

 def get_status(y, x):
      
      if y == '0':
         return x
      else:
         return y

 request['statused'] = request.apply(lambda row: get_status(row['statused_y'], row['statused_x']), axis = 1)
 
 request = pd.concat([request[request['dateentered'] < str(i_date)][['userid', 'dateentered', 'phone', 'queue', 'regions', 'datecall', 'result_call_c', 'otkaz_c', 'duration_minutes',
                               'city', 'town', 'dialog', 'destination_queue', 'city_c', 'Область_y', 'RTK_region', 'TTK_region', 'MTS_region',
                               'BLN_region', 'statused']], requestcitys2[requestcitys2['dateentered'] >= i_date]]).reset_index()
 print(request[(request['statused'] == 'Held') | (request['statused'] == 'Active')].groupby('statused')[['dateentered']].count())


 request.to_csv(f'{path_to_request}/{file_request_result}', index=False, 
                                sep=',',
                                encoding='utf-8')

 calltransfer['city_c'] = calltransfer['city_c'].astype(str)
 calltransfer['city_c'] = calltransfer['city_c'].apply(lambda x: x.replace('.0', ''))
 calltransfer['town'] = calltransfer['town'].astype(str)
 calltransfer['town'] = calltransfer['town'].apply(lambda x: x.replace('.0', ''))

 print('calltransfer: col city_c type', calltransfer['city_c'].dtype, 'col town type', calltransfer['town'].dtype)
 print('city1: col city_c type', city1['Город'].dtype)
 print('city2: col town type', city2['city_c'].dtype)
 print('city4: col town_c type', city4['town_c'].dtype)

 callscitys = calltransfer.merge(city1, how='left', left_on='city', right_on='Город')
 print('merge with city1 finished')
 print('size new df: ', callscitys.shape[0])
 callscitys1 = callscitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
 print('merge with city2 finished')
 print('size new df: ', callscitys1.shape[0])
 callscitys2 = callscitys1.merge(city4, how='left', left_on='town', right_on='town_c')
 print('merge with city4 finished')
 print('size new df: ', callscitys2.shape[0])
 print(callscitys2['dialog'].unique())
 
 callscitys2.drop(['datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'Город_x', 'Город_y', 'queue_c'], axis=1,
                 inplace=True)

 callscitys2.rename(
    columns={'Регион': 'RTK_city', 'Регион ТТК': 'TTK_city', 'Регионы МТС': 'MTS_city', 'Регион Билайн': 'BLN_city',
             'ТТК Регион_x': 'TTK_city_c', 'РТК Регион_x': 'RTK_city_c', 'МТС Регион_x': 'MTS_city_c',
             'Билайн Регион_x': 'BLN_city_c', 'ТТК Регион_y': 'TTK_town', 'РТК Регион_y': 'RTK_town',
             'Билайн Регион_y': 'BLN_town', 'МТС Регион_y': 'MTS_town'}, inplace=True)

 callscitys2 = callscitys2.fillna('0')
 callscitys2['RTK_region'] = callscitys2.apply(lambda row: defs.region_defination(row['RTK_city'], 
                                                                                 row['RTK_city_c'],
                                                                                 row['RTK_town']), axis=1)
 print('РТК регион')
 callscitys2['TTK_region'] = callscitys2.apply(lambda row: defs.region_defination(row['TTK_city'], 
                                                                                 row['TTK_city_c'],
                                                                                 row['TTK_town']), axis=1) 
 print('ТТК регион')
 callscitys2['MTS_region'] = callscitys2.apply(lambda row: defs.region_defination(row['MTS_city'], 
                                                                                 row['MTS_city_c'],
                                                                                 row['MTS_town']), axis=1) 
 print('МТС регион')
 callscitys2['BLN_region'] = callscitys2.apply(lambda row: defs.region_defination(row['BLN_city'], 
                                                                                 row['BLN_city_c'],
                                                                                 row['BLN_town']), axis=1) 

 callscitys2.drop(
    ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
     'RTK_town', 'MTS_town', 'BLN_town', 'city_guess', 'town_guess'], axis=1, inplace=True)

 callscitys2 = callscitys2.drop_duplicates().fillna('')
 
 callscitys2.to_csv(f'{path_result}/{file_call_result}',sep=',', index=False)

































# def region_editer(path_to_files, requests, path_result, file_result_req,file_result):
#  import pandas as pd
#  import pymysql
#  import gspread
#  from oauth2client.service_account import ServiceAccountCredentials
#  import datetime
#  import os
#  import glob
#  import indicators_to_regions.defs as defs


#  print('start proccess')
#  requests = pd.read_csv(f'{path_to_files}/{requests}')
#  print('read call')

#  csv_files = glob.glob('/root/airflow/dags/indicators_to_regions/Files/sql_files/callls/*.csv')
#  dataframes = []

#  for file in csv_files:
#     df = pd.read_csv(file)
#     dataframes.append(df)

#  calls = pd.concat(dataframes)
#  print('concat call succesfull')

#  calls.reset_index(drop=True, inplace=True)
#  print('read transfer')

#  csv_files = glob.glob('/root/airflow/dags/indicators_to_regions/Files/transfer/*.csv')
#  dataframes = []

#  for file in csv_files:
#     df = pd.read_csv(file)
#     dataframes.append(df)

#  transferfull = pd.concat(dataframes)
#  print('__concat transfer succesfull')

#  transferfull = transferfull.drop_duplicates(subset=['phone', 'datecalls'], keep='last')
#  print('__delete duplicates')
#  transferfull.reset_index(drop=True, inplace=True)
#  print('size transfer: ', transferfull.shape[0])
#  print('size call: ', calls.shape[0])

#  print('start change types data')

#  calls['datecall'] = calls['datecall'].astype(str)
#  calls['hoursonly'] = calls['hoursonly'].astype(str).apply(lambda x: x.replace('.0', ''))
#  calls['phone'] = calls['phone'].astype(str).apply(lambda x: x.replace('.0', ''))
#  calls['queue_c'] = calls['queue_c'].fillna(0).astype(int).astype(str)
#  calls['city_c'] = calls['city_c'].fillna(0).astype(int).astype(str)
 
#  transferfull['datecalls'] = transferfull['datecalls'].astype(str)    
#  transferfull['hoursonly'] = transferfull['hoursonly'].astype(str).apply(lambda x: x.replace('.0', ''))
#  transferfull['phone'] =transferfull['phone'].astype(str).apply(lambda x: x.replace('.0', ''))
#  transferfull['dialog'] = transferfull['dialog'].fillna(0).astype(str)
#  transferfull['destination_queue'] = transferfull['destination_queue'].fillna(0).astype(str)
#  transferfull['city_c'] = transferfull['city_c'].fillna(0).astype(int).astype(str)
#  transferfull['town'] = transferfull['town'].fillna(0).astype(int).astype(str)

#  requests['contact'] = requests['contact'].astype(str).apply(lambda x: x.replace('.0', ''))

#  print('datatypes changed')

#  calltransfer = calls.merge(transferfull, how='left', left_on=['phone', 'datecall', 'hoursonly', 'queue_c'],
#                            right_on=['phone', 'datecalls', 'hoursonly', 'dialog'])
#  print('merge calls and transfer finish')
#  print('size new df ', calltransfer.shape[0])

#  calltransfer['city_c_x'] = calltransfer['city_c_x'].fillna('0').astype('int64')
#  calltransfer['city_c_y'] = calltransfer['city_c_y'].fillna('0').astype('int64')

#  calltransfer['city_c'] = calltransfer.apply(lambda row: defs.area_defination(row['city_c_x'], row['city_c_y']), axis=1)

#  del calltransfer['city_c_x']
#  del calltransfer['city_c_y']

#  print('defination city_c complete')
 
#  calltransfer['town'] = calltransfer['town'].fillna('0').astype('int64')
 
#  calltransfer['city'] = calltransfer['city'].fillna('0')

#  calltransfer['city_guess'] = calltransfer.sort_values(['datecall', 'phone', 'city_c'], ascending=False).\
#                                           groupby(['datecall', 'phone'])['city_c'].cummax()
#  calltransfer['town_guess'] = calltransfer.sort_values(['datecall', 'phone', 'town'], ascending=False).\
#                                           groupby(['datecall', 'phone'])['town'].cummax()
#  print('create columns guess town and city')

#  calltransfer['city_c'] = calltransfer.apply(lambda row: defs.area_defination(row['city_c'], row['city_guess']), axis = 1).astype(str)
#  calltransfer['town'] = calltransfer.apply(lambda row: defs.area_defination(row['town'], row['town_guess']), axis = 1).astype(str)
#  print('city_c and town was definated')

#  call_noduplicates = calltransfer.sort_values(['datecall', 'phone', 'city_c', 'town']).drop_duplicates(subset=['phone', 'userid'], keep='last')

#  print('start merge with request')
#  print('size_request: ', requests.shape[0])
 

#  requests = requests.merge(call_noduplicates, how='left', left_on=['contact', 'userid'], right_on=['phone', 'userid'])

#  print('merge with request finished')
#  print('size new df: ', requests.shape[0])
#  path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json'

#  scope = ['https://spreadsheets.google.com/feeds',
#          'https://www.googleapis.com/auth/drive']

#  credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
#  gs = gspread.authorize(credentials)

#  table_name4 = 'Команды/Проекты'

#  work_sheet4 = gs.open(table_name4)
#  sheet4 = work_sheet4.worksheet('Регионы-Города')
#  data4 = sheet4.get_all_values()
#  headers4 = data4.pop(0)
#  city1 = pd.DataFrame(data4, columns=headers4)
#  print('size city1 ', city1.shape[0])
#  city1 = city1.drop_duplicates(subset='Город', keep='first', inplace=False)
#  print('size city1 after drop duplicates', city1.shape[0])

#  city2 = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv', sep=',', encoding='utf-8')

#  city3 = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv', sep=',', encoding='utf-8')

#  city4 = city3.drop_duplicates(subset='town_c', keep='first', inplace=False)

#  city4 = city4.drop(['city_c', 'Город', 'MACRO'], axis=1)
#  city2['city_c'] = city2['city_c'].astype(str)
#  city4['town_c'] = city4['town_c'].astype(str)
#  print('Все файлы для соединения полностью прочитаны')
#  print('requests: col city_c type before changed ', requests['city_c'].dtype, 'col town type', requests['town'].dtype)

#  requests['city_c'] = requests['city_c'].astype(str).apply(lambda x: x.replace('.0', ''))
#  requests['town'] = requests['town'].astype(str).apply(lambda x: x.replace('.0', ''))
#  requests['queue'] = requests['queue'].astype(str).apply(lambda x: x.replace('.0', ''))
 
#  print('reqauest: col city_c type after changed ', requests['city_c'].dtype, 'col town type', requests['town'].dtype)
#  print('city1: col city_c type', city1['Город'].dtype)
#  print('city2: col town type', city2['city_c'].dtype)
#  print('city4: col town_c type', city4['town_c'].dtype)
#  print('count of city not null in request: ', requests[requests['city'] != '0']['city'].count())

#  requestcitys = requests.merge(city1, how='left', left_on='city', right_on='Город')
#  print('merge with city1 finished')
#  print('size new df: ', requestcitys.shape[0])
#  requestcitys['Регион'] = requestcitys['Регион'].fillna('0')
 
#  print('count of Город: ', requestcitys[requestcitys['Регион'] != '0']['Регион'].count())
 

#  requestcitys1 = requestcitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
#  print('merge with city2 finished')
#  print('size new df: ', requestcitys1.shape[0])
#  requestcitys1['Город_y'] = requestcitys1['Город_y'].fillna('0')
#  print('count of Город: ', requestcitys1[requestcitys1['Город_y'] != '0'].count())

#  requestcitys2 = requestcitys1.merge(city4, how='left', left_on='town', right_on='town_c')
#  print('merge with city4 finished')
#  print('size new df: ', requestcitys2.shape[0])
#  requestcitys2['Область_y'] = requestcitys2['Область_y'].fillna('0')
#  print('count of Город: ', requestcitys2[requestcitys2['Область_y'] != '0'].count())

#  requestcitys2.drop(
#     ['contact', 'datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'MACRO', 'Город_x', 'Город_y', 'queue_c', 'hoursonly',
#      'project_c'], axis=1, inplace=True)

#  requestcitys2.rename(
#     columns={'Регион': 'RTK_city',
#               'Регион ТТК': 'TTK_city',
#               'Регионы МТС': 'MTS_city',
#               'Регион Билайн': 'BLN_city',
#               'ТТК Регион_x': 'TTK_city_c',
#               'РТК Регион_x': 'RTK_city_c',
#               'МТС Регион_x': 'MTS_city_c',
#               'Билайн Регион_x': 'BLN_city_c',
#               'ТТК Регион_y': 'TTK_town',
#               'РТК Регион_y': 'RTK_town',
#               'Билайн Регион_y': 'BLN_town',
#               'МТС Регион_y': 'MTS_town'}, inplace=True)
 
#  col_list = ['userid',
#             'statused',
#             'queue', 
#             'regions',
#             'result_call_c',
#             'otkaz_c', 
#             'phone', 
#             'duration_minutes',
#             'city', 
#             'town', 
#             'dialog', 
#             'destination_queue', 
#             'city_c', 
#             'Область_y', 
#             'RTK_city',
#             'TTK_city',
#             'MTS_city',
#             'BLN_city',
#             'TTK_city_c',
#             'RTK_city_c',
#             'MTS_city_c',
#             'BLN_city_c',
#             'TTK_town',
#             'RTK_town',
#             'BLN_town',
#             'MTS_town']
 
#  for col in col_list:
#    requestcitys2[col] = requestcitys2[col].fillna('0')

#  requestcitys2['dateentered'] = requestcitys2['dateentered'].fillna('')
#  requestcitys2['datecall'] = requestcitys2['datecall'].fillna('')

# #  requestcitys2['userid', 'statused', 'queue', 'regions', 'result_call_c', 'otkaz_c', 'phone', 'duration_minutes',
# #        'city', 'town', 'dialog', 'destination_queue', 'city_c', 'Область_y', 'RTK_region', 'TTK_region', 'MTS_region', 'BLN_region'] =\
# #            requestcitys2['userid', 'statused', 'queue', 'regions', 'result_call_c', 'otkaz_c', 'phone', 'duration_minutes',
# #        'city', 'town', 'dialog', 'destination_queue', 'city_c', 'Область_y', 'RTK_region', 'TTK_region', 'MTS_region', 'BLN_region'].fillna('0')
 
# #  requestcitys2['dateentered', 'datecall'] = requestcitys2['dateentered', 'datecall'].fillna('')

#  requestcitys2['RTK_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['RTK_city'], 
#                                                                                        row['RTK_city_c'],
#                                                                                        row['RTK_town']), axis=1) 
#  requestcitys2['TTK_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['TTK_city'], 
#                                                                                        row['TTK_city_c'],
#                                                                                        row['TTK_town']), axis=1) 
#  requestcitys2['MTS_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['MTS_city'], 
#                                                                                        row['MTS_city_c'],
#                                                                                        row['MTS_town']), axis=1) 
#  requestcitys2['BLN_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['BLN_city'], 
#                                                                                        row['BLN_city_c'],
#                                                                                        row['BLN_town']), axis=1) 
 
#  requestcitys2.drop(
#     ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
#      'RTK_town', 'MTS_town', 'BLN_town', 'city_guess', 'town_guess'], axis=1, inplace=True)

#  requestcitys2.to_csv(f'{path_result}/{file_result_req}', index=False,
#                                 sep=',',
#                                 encoding='utf-8')

#  print('size request df: ', requestcitys2.shape[0])
#  print('save request to csv')

#  calltransfer['city_c'] = calltransfer['city_c'].astype(str)
#  calltransfer['city_c'] = calltransfer['city_c'].apply(lambda x: x.replace('.0', ''))
#  calltransfer['town'] = calltransfer['town'].astype(str)
#  calltransfer['town'] = calltransfer['town'].apply(lambda x: x.replace('.0', ''))

#  print('calltransfer: col city_c type', calltransfer['city_c'].dtype, 'col town type', calltransfer['town'].dtype)
#  print('city1: col city_c type', city1['Город'].dtype)
#  print('city2: col town type', city2['city_c'].dtype)
#  print('city4: col town_c type', city4['town_c'].dtype)

#  callscitys = calltransfer.merge(city1, how='left', left_on='city', right_on='Город')
#  print('merge with city1 finished')
#  print('size new df: ', callscitys.shape[0])
#  callscitys1 = callscitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
#  print('merge with city2 finished')
#  print('size new df: ', callscitys1.shape[0])
#  callscitys2 = callscitys1.merge(city4, how='left', left_on='town', right_on='town_c')
#  print('merge with city4 finished')
#  print('size new df: ', callscitys2.shape[0])

#  callscitys2.drop(['datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'MACRO', 'Город_x', 'Город_y', 'queue_c'], axis=1,
#                  inplace=True)

#  callscitys2.rename(
#     columns={'Регион': 'RTK_city', 'Регион ТТК': 'TTK_city', 'Регионы МТС': 'MTS_city', 'Регион Билайн': 'BLN_city',
#              'ТТК Регион_x': 'TTK_city_c', 'РТК Регион_x': 'RTK_city_c', 'МТС Регион_x': 'MTS_city_c',
#              'Билайн Регион_x': 'BLN_city_c', 'ТТК Регион_y': 'TTK_town', 'РТК Регион_y': 'RTK_town',
#              'Билайн Регион_y': 'BLN_town', 'МТС Регион_y': 'MTS_town'}, inplace=True)

#  callscitys2 = callscitys2.fillna('0')
#  callscitys2['RTK_region'] = callscitys2.apply(lambda row: defs.region_defination(row['RTK_city'], 
#                                                                                  row['RTK_city_c'],
#                                                                                  row['RTK_town']), axis=1)
#  print('РТК регион')
#  callscitys2['TTK_region'] = callscitys2.apply(lambda row: defs.region_defination(row['TTK_city'], 
#                                                                                  row['TTK_city_c'],
#                                                                                  row['TTK_town']), axis=1) 
#  print('ТТК регион')
#  callscitys2['MTS_region'] = callscitys2.apply(lambda row: defs.region_defination(row['MTS_city'], 
#                                                                                  row['MTS_city_c'],
#                                                                                  row['MTS_town']), axis=1) 
#  print('МТС регион')
#  callscitys2['BLN_region'] = callscitys2.apply(lambda row: defs.region_defination(row['BLN_city'], 
#                                                                                  row['BLN_city_c'],
#                                                                                  row['BLN_town']), axis=1) 

#  callscitys2.drop(
#     ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
#      'RTK_town', 'MTS_town', 'BLN_town', 'city_guess', 'town_guess'], axis=1, inplace=True)

#  callscitys2 = callscitys2.drop_duplicates().fillna('')
#  print('save callcitys to csv')

#  callscitys2.to_csv(f'{path_result}/{file_result}',sep=',', index=False)

# def region_editer(path_to_files, requests, path_result, file_result_req,file_result):
#  import pandas as pd
#  import pymysql
#  import gspread
#  from oauth2client.service_account import ServiceAccountCredentials
#  import datetime
#  import os
#  import glob
#  import indicators_to_regions.defs as defs


#  print('start proccess')
#  requests = pd.read_csv(f'{path_to_files}/{requests}')
#  print('read call')

#  csv_files = glob.glob('/root/airflow/dags/indicators_to_regions/Files/sql_files/callls/*.csv')
#  dataframes = []

#  for file in csv_files:
#     df = pd.read_csv(file)
#     dataframes.append(df)

#  calls = pd.concat(dataframes)
#  print('concat call succesfull')

#  calls.reset_index(drop=True, inplace=True)
#  print('read transfer')

#  csv_files = glob.glob('/root/airflow/dags/indicators_to_regions/Files/transfer/*.csv')
#  dataframes = []

#  for file in csv_files:
#     df = pd.read_csv(file)
#     dataframes.append(df)

#  transferfull = pd.concat(dataframes)
#  print('__concat transfer succesfull')

#  transferfull = transferfull.drop_duplicates(subset=['phone', 'datecalls'], keep='last')
#  print('__delete duplicates')
#  transferfull.reset_index(drop=True, inplace=True)
#  print('size transfer: ', transferfull.shape[0])
#  print('size call: ', calls.shape[0])


#  print('request.contact: ', requests.contact.dtype)

#  print('start change types data')
#  calls['datecall'] = calls['datecall'].astype(str)
#  calls['hoursonly'] = calls['hoursonly'].astype(str)
#  calls['hoursonly'] = calls['hoursonly'].apply(lambda x: x.replace('.0', ''))
#  calls['phone'] = calls['phone'].astype(str)
#  calls['phone'] = calls['phone'].apply(lambda x: x.replace('.0', ''))
#  calls['queue_c'] = calls['queue_c'].astype(str)
#  calls['queue_c'] = calls['queue_c'].apply(lambda x: x.replace('.0', ''))


#  transferfull['datecalls'] = transferfull['datecalls'].astype(str)    
#  transferfull['hoursonly'] = transferfull['hoursonly'].astype(str)
#  transferfull['hoursonly'] = transferfull['hoursonly'].apply(lambda x: x.replace('.0', ''))
#  transferfull['phone'] = transferfull['phone'].astype(str)
#  transferfull['phone'] = transferfull['phone'].apply(lambda x: x.replace('.0', ''))
#  transferfull['dialog'] = transferfull['dialog'].astype(str)
#  transferfull['dialog'] = transferfull['dialog'].apply(lambda x: x.replace('.0', ''))
#  transferfull['destination_queue'] = transferfull['destination_queue'].astype(str)
#  transferfull['destination_queue'] = transferfull['destination_queue'].apply(lambda x: x.replace('.0', ''))
#  transferfull['city_c'] = transferfull['city_c'].astype(str)
#  transferfull['city_c'] = transferfull['city_c'].apply(lambda x: x.replace('.0', ''))
#  transferfull['town'] = transferfull['town'].astype(str)
#  transferfull['town'] = transferfull['town'].apply(lambda x: x.replace('.0', ''))
#  requests['contact'] = requests['contact'].astype(str)
#  requests['contact'] = requests['contact'].apply(lambda x: x.replace('.0', ''))

# #  print('call columns: ', calls.columns)
# #  print('transfer columns: ', transferfull.columns)
# #  print(transferfull.head(10))
# #  print(calls.head(10))
# #  print('Переводы полностью прочитаны')
# #  calls.loc[calls['phone'] == '89922404564']
# #  transferfull.loc[transferfull['phone'] == '89922404564']
#  calltransfer = calls.merge(transferfull, how='left', left_on=['phone', 'datecall', 'hoursonly', 'queue_c'],
#                            right_on=['phone', 'datecalls', 'hoursonly', 'dialog'])
# #  print(calltransfer.head(10))
# #  calltransfer.loc[calltransfer['phone'] == '89922404564']
#  print('merge calls and transfer finish')
#  print('size new df ', calltransfer.shape[0])

#  calltransfer['city_c_x'] = calltransfer['city_c_x'].fillna('')
#  calltransfer['city_c_y'] = calltransfer['city_c_y'].fillna('')

#  calltransfer['city_c'] = calltransfer.apply(lambda row: defs.area_defination(row['city_c_y'], row['city_c_x']), axis=1)

#  del calltransfer['city_c_x']
#  del calltransfer['city_c_y']

#  print('defination city_c complete')

#  calltransfer['phone'] = calltransfer['phone'].astype(str)
#  calltransfer['phone'] = calltransfer['phone'].apply(lambda x: x.replace('.0', ''))

# #  call_noduplicates = calltransfer.sort_values(['datecall', 'duration_minutes']).drop_duplicates(subset=['phone', 'userid'], keep='last')

#  print('start merge with request')
#  print('size_request: ', requests.shape[0])

#  requests = requests.merge(calltransfer, how='left', left_on=['contact', 'userid'], right_on=['phone', 'userid'])
 
# #  print(requests.head(10))
# #  requests.loc[requests['contact'] == '89922404564']

#  print('merge with request finished')
#  print('size new df: ', requests.shape[0])
#  path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json'

#  scope = ['https://spreadsheets.google.com/feeds',
#          'https://www.googleapis.com/auth/drive']

#  credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
#  gs = gspread.authorize(credentials)

#  table_name4 = 'Команды/Проекты'

#  work_sheet4 = gs.open(table_name4)
#  sheet4 = work_sheet4.worksheet('Регионы-Города')
#  data4 = sheet4.get_all_values()
#  headers4 = data4.pop(0)
#  city1 = pd.DataFrame(data4, columns=headers4)
#  print('size city1 ', city1.shape[0])
#  city1 = city1.drop_duplicates(subset='Город', keep='first', inplace=False)
#  print('size city1 after drop duplicates', city1.shape[0])

#  city2 = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv', sep=',', encoding='utf-8')

#  city3 = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv', sep=',', encoding='utf-8')

#  city4 = city3.drop_duplicates(subset='town_c', keep='first', inplace=False)

#  city4 = city4.drop(['city_c', 'Город', 'MACRO'], axis=1)
#  city2['city_c'] = city2['city_c'].astype(str)
#  city4['town_c'] = city4['town_c'].astype(str)
#  print('Все файлы для соединения полностью прочитаны')

#  requests['city_c'] = requests['city_c'].astype(str)
#  requests['city_c'] = requests['city_c'].apply(lambda x: x.replace('.0', ''))
#  requests['town'] = requests['town'].astype(str)
#  requests['town'] = requests['town'].apply(lambda x: x.replace('.0', ''))
#  requestcitys = requests.merge(city1, how='left', left_on='city', right_on='Город')
#  print('merge with city1 finished')
#  print('size new df: ', requestcitys.shape[0])

#  requestcitys1 = requestcitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
#  print('merge with city2 finished')
#  print('size new df: ', requestcitys1.shape[0])

#  requestcitys2 = requestcitys1.merge(city4, how='left', left_on='town', right_on='town_c')
#  print('merge with city4 finished')
#  print('size new df: ', requestcitys2.shape[0])


#  requestcitys2.drop(
#     ['contact', 'datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'MACRO', 'Город_x', 'Город_y', 'queue_c', 'hoursonly',
#      'project_c'], axis=1, inplace=True)

#  requestcitys2.rename(
#     columns={'Регион': 'RTK_city', 'Регион ТТК': 'TTK_city', 'Регионы МТС': 'MTS_city', 'Регион Билайн': 'BLN_city',
#              'ТТК Регион_x': 'TTK_city_c', 'РТК Регион_x': 'RTK_city_c', 'МТС Регион_x': 'MTS_city_c',
#              'Билайн Регион_x': 'BLN_city_c', 'ТТК Регион_y': 'TTK_town', 'РТК Регион_y': 'RTK_town',
#              'Билайн Регион_y': 'BLN_town', 'МТС Регион_y': 'MTS_town'}, inplace=True)

#  requestcitys3 = requestcitys2.fillna('')
#  requestcitys3['RTK_region'] = requestcitys3.apply(lambda row: defs.rtk_reg_r(row), axis=1) 
#  requestcitys3['TTK_region'] = requestcitys3.apply(lambda row: defs.ttk_reg_r(row), axis=1)
#  requestcitys3['MTS_region'] = requestcitys3.apply(lambda row: defs.mts_reg_r(row), axis=1)
#  requestcitys3['BLN_region'] = requestcitys3.apply(lambda row: defs.bln_reg_r(row), axis=1)
 
#  requestcitys3 = requestcitys3.fillna('')

#  requestcitys3.drop(
#     ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
#      'RTK_town', 'MTS_town', 'BLN_town'], axis=1, inplace=True)

#  requestcitys3.fillna('').to_csv(f'{path_result}/{file_result_req}', index=False,
#                                 sep=',',
#                                 encoding='utf-8')

#  print('size request df: ', requestcitys3.shape[0])
 

#  print('save request to csv')

#  calltransfer['city_c'] = calltransfer['city_c'].astype(str)
#  calltransfer['city_c'] = calltransfer['city_c'].apply(lambda x: x.replace('.0', ''))
#  calltransfer['town'] = calltransfer['town'].astype(str)
#  calltransfer['town'] = calltransfer['town'].apply(lambda x: x.replace('.0', ''))

#  print('calltransfer: col city_c type', calltransfer['city_c'].dtype, 'col town type', calltransfer['town'].dtype)
#  print('city1: col city_c type', city1['Город'].dtype)
#  print('city2: col town type', city2['city_c'].dtype)
#  print('city4: col town_c type', city4['town_c'].dtype)

#  callscitys = calltransfer.merge(city1, how='left', left_on='city', right_on='Город')
#  print('merge with city1 finished')
#  print('size new df: ', callscitys.shape[0])
#  callscitys1 = callscitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
#  print('merge with city2 finished')
#  print('size new df: ', callscitys1.shape[0])
#  callscitys2 = callscitys1.merge(city4, how='left', left_on='town', right_on='town_c')
#  print('merge with city4 finished')
#  print('size new df: ', callscitys2.shape[0])
#  callscitys2.drop(['datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'MACRO', 'Город_x', 'Город_y', 'queue_c'], axis=1,
#                  inplace=True)

#  callscitys2.rename(
#     columns={'Регион': 'RTK_city', 'Регион ТТК': 'TTK_city', 'Регионы МТС': 'MTS_city', 'Регион Билайн': 'BLN_city',
#              'ТТК Регион_x': 'TTK_city_c', 'РТК Регион_x': 'RTK_city_c', 'МТС Регион_x': 'MTS_city_c',
#              'Билайн Регион_x': 'BLN_city_c', 'ТТК Регион_y': 'TTK_town', 'РТК Регион_y': 'RTK_town',
#              'Билайн Регион_y': 'BLN_town', 'МТС Регион_y': 'MTS_town'}, inplace=True)

#  callscitys2 = callscitys2.fillna('')
#  callscitys2['RTK_region'] = callscitys2.apply(lambda row: defs.rtk_reg(row), axis=1)
#  print('РТК регион')
#  callscitys2['TTK_region'] = callscitys2.apply(lambda row: defs.ttk_reg(row), axis=1)
#  print('ТТК регион')
#  callscitys2['MTS_region'] = callscitys2.apply(lambda row: defs.mts_reg(row), axis=1)
#  print('МТС регион')
#  callscitys2['BLN_region'] = callscitys2.apply(lambda row: defs.bln_reg(row), axis=1)
#  print('Билайн регион')
 
#  callscitys2 = callscitys2.fillna('')

#  callscitys2.drop(
#     ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
#      'RTK_town', 'MTS_town', 'BLN_town'], axis=1, inplace=True)

#  callscitys2 = callscitys2.drop_duplicates().fillna('')
#  print('save callcitys to csv')

#  callscitys2.to_csv(f'{path_result}/{file_result}',sep=',', index=False)



































# def region_editer(path_to_files, requests, path_result, file_result_req,file_result):
#  import pandas as pd
#  import pymysql
#  import gspread
#  from oauth2client.service_account import ServiceAccountCredentials
#  import datetime
#  import os
#  import glob
#  import indicators_to_regions.defs as defs


#  print('start proccess')
#  requests = pd.read_csv(f'{path_to_files}/{requests}')
#  print('read call')

#  csv_files = glob.glob('/root/airflow/dags/indicators_to_regions/Files/sql_files/callls/*.csv')
#  dataframes = []

#  for file in csv_files:
#     df = pd.read_csv(file)
#     dataframes.append(df)

#  calls = pd.concat(dataframes)
#  print('concat call succesfull')

#  calls.reset_index(drop=True, inplace=True)
#  print('read transfer')

#  csv_files = glob.glob('/root/airflow/dags/indicators_to_regions/Files/transfer/*.csv')
#  dataframes = []

#  for file in csv_files:
#     df = pd.read_csv(file)
#     dataframes.append(df)

#  transferfull = pd.concat(dataframes)
#  print('__concat transfer succesfull')

#  transferfull = transferfull.drop_duplicates(subset=['phone', 'datecalls'], keep='last')
#  print('__delete duplicates')
#  transferfull.reset_index(drop=True, inplace=True)
#  print('size transfer: ', transferfull.shape[0])
#  print('size call: ', calls.shape[0])

#  print('start change types data')

#  calls['datecall'] = calls['datecall'].astype(str)
#  calls['hoursonly'] = calls['hoursonly'].astype(str).apply(lambda x: x.replace('.0', ''))
#  calls['phone'] = calls['phone'].astype(str).apply(lambda x: x.replace('.0', ''))
#  calls['queue_c'] = calls['queue_c'].fillna(0).astype(int).astype(str)
#  calls['city_c'] = calls['city_c'].fillna(0).astype(int).astype(str)
 
#  transferfull['datecalls'] = transferfull['datecalls'].astype(str)    
#  transferfull['hoursonly'] = transferfull['hoursonly'].astype(str).apply(lambda x: x.replace('.0', ''))
#  transferfull['phone'] =transferfull['phone'].astype(str).apply(lambda x: x.replace('.0', ''))
#  transferfull['dialog'] = transferfull['dialog'].fillna(0).astype(str)
#  transferfull['destination_queue'] = transferfull['destination_queue'].fillna(0).astype(str)
#  transferfull['city_c'] = transferfull['city_c'].fillna(0).astype(int).astype(str)
#  transferfull['town'] = transferfull['town'].fillna(0).astype(int).astype(str)

#  requests['contact'] = requests['contact'].astype(str).apply(lambda x: x.replace('.0', ''))

#  print('datatypes changed')

#  calltransfer = calls.merge(transferfull, how='left', left_on=['phone', 'datecall', 'hoursonly', 'queue_c'],
#                            right_on=['phone', 'datecalls', 'hoursonly', 'dialog'])
#  print('merge calls and transfer finish')
#  print('size new df ', calltransfer.shape[0])

#  calltransfer['city_c_x'] = calltransfer['city_c_x'].fillna('0').astype('int64')
#  calltransfer['city_c_y'] = calltransfer['city_c_y'].fillna('0').astype('int64')

#  calltransfer['city_c'] = calltransfer.apply(lambda row: defs.area_defination(row['city_c_x'], row['city_c_y']), axis=1)

#  del calltransfer['city_c_x']
#  del calltransfer['city_c_y']

#  print('defination city_c complete')
 
#  calltransfer['town'] = calltransfer['town'].fillna('0').astype('int64')
 
#  calltransfer['city'] = calltransfer['city'].fillna('0')

#  calltransfer['city_guess'] = calltransfer.sort_values(['datecall', 'phone', 'city_c'], ascending=False).\
#                                           groupby(['datecall', 'phone'])['city_c'].cummax()
#  calltransfer['town_guess'] = calltransfer.sort_values(['datecall', 'phone', 'town'], ascending=False).\
#                                           groupby(['datecall', 'phone'])['town'].cummax()
#  print('create columns guess town and city')

#  calltransfer['city_c'] = calltransfer.apply(lambda row: defs.area_defination(row['city_c'], row['city_guess']), axis = 1).astype(str)
#  calltransfer['town'] = calltransfer.apply(lambda row: defs.area_defination(row['town'], row['town_guess']), axis = 1).astype(str)
#  print('city_c and town was definated')

#  call_noduplicates = calltransfer.sort_values(['datecall', 'phone', 'city_c', 'town']).drop_duplicates(subset=['phone', 'userid'], keep='last')

#  print('start merge with request')
#  print('size_request: ', requests.shape[0])
 

#  requests = requests.merge(call_noduplicates, how='left', left_on=['contact', 'userid'], right_on=['phone', 'userid'])

#  print('merge with request finished')
#  print('size new df: ', requests.shape[0])
#  path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json'

#  scope = ['https://spreadsheets.google.com/feeds',
#          'https://www.googleapis.com/auth/drive']

#  credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
#  gs = gspread.authorize(credentials)

#  table_name4 = 'Команды/Проекты'

#  work_sheet4 = gs.open(table_name4)
#  sheet4 = work_sheet4.worksheet('Регионы-Города')
#  data4 = sheet4.get_all_values()
#  headers4 = data4.pop(0)
#  city1 = pd.DataFrame(data4, columns=headers4)
#  print('size city1 ', city1.shape[0])
#  city1 = city1.drop_duplicates(subset='Город', keep='first', inplace=False)
#  print('size city1 after drop duplicates', city1.shape[0])

#  city2 = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv', sep=',', encoding='utf-8')

#  city3 = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv', sep=',', encoding='utf-8')

#  city4 = city3.drop_duplicates(subset='town_c', keep='first', inplace=False)

#  city4 = city4.drop(['city_c', 'Город', 'MACRO'], axis=1)
#  city2['city_c'] = city2['city_c'].astype(str)
#  city4['town_c'] = city4['town_c'].astype(str)
#  print('Все файлы для соединения полностью прочитаны')
#  print('requests: col city_c type before changed ', requests['city_c'].dtype, 'col town type', requests['town'].dtype)

#  requests['city_c'] = requests['city_c'].astype(str).apply(lambda x: x.replace('.0', ''))
#  requests['town'] = requests['town'].astype(str).apply(lambda x: x.replace('.0', ''))
#  requests['queue'] = requests['queue'].astype(str).apply(lambda x: x.replace('.0', ''))
 
#  print('reqauest: col city_c type after changed ', requests['city_c'].dtype, 'col town type', requests['town'].dtype)
#  print('city1: col city_c type', city1['Город'].dtype)
#  print('city2: col town type', city2['city_c'].dtype)
#  print('city4: col town_c type', city4['town_c'].dtype)
#  print('count of city not null in request: ', requests[requests['city'] != '0']['city'].count())

#  requestcitys = requests.merge(city1, how='left', left_on='city', right_on='Город')
#  print('merge with city1 finished')
#  print('size new df: ', requestcitys.shape[0])
#  requestcitys['Регион'] = requestcitys['Регион'].fillna('0')
 
#  print('count of Город: ', requestcitys[requestcitys['Регион'] != '0']['Регион'].count())
 

#  requestcitys1 = requestcitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
#  print('merge with city2 finished')
#  print('size new df: ', requestcitys1.shape[0])
#  requestcitys1['Город_y'] = requestcitys1['Город_y'].fillna('0')
#  print('count of Город: ', requestcitys1[requestcitys1['Город_y'] != '0'].count())

#  requestcitys2 = requestcitys1.merge(city4, how='left', left_on='town', right_on='town_c')
#  print('merge with city4 finished')
#  print('size new df: ', requestcitys2.shape[0])
#  requestcitys2['Область_y'] = requestcitys2['Область_y'].fillna('0')
#  print('count of Город: ', requestcitys2[requestcitys2['Область_y'] != '0'].count())

#  requestcitys2.drop(
#     ['contact', 'datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'MACRO', 'Город_x', 'Город_y', 'queue_c', 'hoursonly',
#      'project_c'], axis=1, inplace=True)

#  requestcitys2.rename(
#     columns={'Регион': 'RTK_city',
#               'Регион ТТК': 'TTK_city',
#               'Регионы МТС': 'MTS_city',
#               'Регион Билайн': 'BLN_city',
#               'ТТК Регион_x': 'TTK_city_c',
#               'РТК Регион_x': 'RTK_city_c',
#               'МТС Регион_x': 'MTS_city_c',
#               'Билайн Регион_x': 'BLN_city_c',
#               'ТТК Регион_y': 'TTK_town',
#               'РТК Регион_y': 'RTK_town',
#               'Билайн Регион_y': 'BLN_town',
#               'МТС Регион_y': 'MTS_town'}, inplace=True)
 
#  col_list = ['userid',
#             'statused',
#             'queue', 
#             'regions',
#             'result_call_c',
#             'otkaz_c', 
#             'phone', 
#             'duration_minutes',
#             'city', 
#             'town', 
#             'dialog', 
#             'destination_queue', 
#             'city_c', 
#             'Область_y', 
#             'RTK_city',
#             'TTK_city',
#             'MTS_city',
#             'BLN_city',
#             'TTK_city_c',
#             'RTK_city_c',
#             'MTS_city_c',
#             'BLN_city_c',
#             'TTK_town',
#             'RTK_town',
#             'BLN_town',
#             'MTS_town']
 
#  for col in col_list:
#    requestcitys2[col] = requestcitys2[col].fillna('0')

#  requestcitys2['dateentered'] = requestcitys2['dateentered'].fillna('')
#  requestcitys2['datecall'] = requestcitys2['datecall'].fillna('')

# #  requestcitys2['userid', 'statused', 'queue', 'regions', 'result_call_c', 'otkaz_c', 'phone', 'duration_minutes',
# #        'city', 'town', 'dialog', 'destination_queue', 'city_c', 'Область_y', 'RTK_region', 'TTK_region', 'MTS_region', 'BLN_region'] =\
# #            requestcitys2['userid', 'statused', 'queue', 'regions', 'result_call_c', 'otkaz_c', 'phone', 'duration_minutes',
# #        'city', 'town', 'dialog', 'destination_queue', 'city_c', 'Область_y', 'RTK_region', 'TTK_region', 'MTS_region', 'BLN_region'].fillna('0')
 
# #  requestcitys2['dateentered', 'datecall'] = requestcitys2['dateentered', 'datecall'].fillna('')

#  requestcitys2['RTK_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['RTK_city'], 
#                                                                                        row['RTK_city_c'],
#                                                                                        row['RTK_town']), axis=1) 
#  requestcitys2['TTK_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['TTK_city'], 
#                                                                                        row['TTK_city_c'],
#                                                                                        row['TTK_town']), axis=1) 
#  requestcitys2['MTS_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['MTS_city'], 
#                                                                                        row['MTS_city_c'],
#                                                                                        row['MTS_town']), axis=1) 
#  requestcitys2['BLN_region'] = requestcitys2.apply(lambda row: defs.region_defination(row['BLN_city'], 
#                                                                                        row['BLN_city_c'],
#                                                                                        row['BLN_town']), axis=1) 
 
#  requestcitys2.drop(
#     ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
#      'RTK_town', 'MTS_town', 'BLN_town', 'city_guess', 'town_guess'], axis=1, inplace=True)

#  requestcitys2.to_csv(f'{path_result}/{file_result_req}', index=False,
#                                 sep=',',
#                                 encoding='utf-8')

#  print('size request df: ', requestcitys2.shape[0])
#  print('save request to csv')

#  calltransfer['city_c'] = calltransfer['city_c'].astype(str)
#  calltransfer['city_c'] = calltransfer['city_c'].apply(lambda x: x.replace('.0', ''))
#  calltransfer['town'] = calltransfer['town'].astype(str)
#  calltransfer['town'] = calltransfer['town'].apply(lambda x: x.replace('.0', ''))

#  print('calltransfer: col city_c type', calltransfer['city_c'].dtype, 'col town type', calltransfer['town'].dtype)
#  print('city1: col city_c type', city1['Город'].dtype)
#  print('city2: col town type', city2['city_c'].dtype)
#  print('city4: col town_c type', city4['town_c'].dtype)

#  callscitys = calltransfer.merge(city1, how='left', left_on='city', right_on='Город')
#  print('merge with city1 finished')
#  print('size new df: ', callscitys.shape[0])
#  callscitys1 = callscitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
#  print('merge with city2 finished')
#  print('size new df: ', callscitys1.shape[0])
#  callscitys2 = callscitys1.merge(city4, how='left', left_on='town', right_on='town_c')
#  print('merge with city4 finished')
#  print('size new df: ', callscitys2.shape[0])

#  callscitys2.drop(['datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'MACRO', 'Город_x', 'Город_y', 'queue_c'], axis=1,
#                  inplace=True)

#  callscitys2.rename(
#     columns={'Регион': 'RTK_city', 'Регион ТТК': 'TTK_city', 'Регионы МТС': 'MTS_city', 'Регион Билайн': 'BLN_city',
#              'ТТК Регион_x': 'TTK_city_c', 'РТК Регион_x': 'RTK_city_c', 'МТС Регион_x': 'MTS_city_c',
#              'Билайн Регион_x': 'BLN_city_c', 'ТТК Регион_y': 'TTK_town', 'РТК Регион_y': 'RTK_town',
#              'Билайн Регион_y': 'BLN_town', 'МТС Регион_y': 'MTS_town'}, inplace=True)

#  callscitys2 = callscitys2.fillna('0')
#  callscitys2['RTK_region'] = callscitys2.apply(lambda row: defs.region_defination(row['RTK_city'], 
#                                                                                  row['RTK_city_c'],
#                                                                                  row['RTK_town']), axis=1)
#  print('РТК регион')
#  callscitys2['TTK_region'] = callscitys2.apply(lambda row: defs.region_defination(row['TTK_city'], 
#                                                                                  row['TTK_city_c'],
#                                                                                  row['TTK_town']), axis=1) 
#  print('ТТК регион')
#  callscitys2['MTS_region'] = callscitys2.apply(lambda row: defs.region_defination(row['MTS_city'], 
#                                                                                  row['MTS_city_c'],
#                                                                                  row['MTS_town']), axis=1) 
#  print('МТС регион')
#  callscitys2['BLN_region'] = callscitys2.apply(lambda row: defs.region_defination(row['BLN_city'], 
#                                                                                  row['BLN_city_c'],
#                                                                                  row['BLN_town']), axis=1) 

#  callscitys2.drop(
#     ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
#      'RTK_town', 'MTS_town', 'BLN_town', 'city_guess', 'town_guess'], axis=1, inplace=True)

#  callscitys2 = callscitys2.drop_duplicates().fillna('')
#  print('save callcitys to csv')

#  callscitys2.to_csv(f'{path_result}/{file_result}',sep=',', index=False)

# def region_editer(path_to_files, requests, path_result, file_result_req,file_result):
#  import pandas as pd
#  import pymysql
#  import gspread
#  from oauth2client.service_account import ServiceAccountCredentials
#  import datetime
#  import os
#  import glob
#  import indicators_to_regions.defs as defs


#  print('start proccess')
#  requests = pd.read_csv(f'{path_to_files}/{requests}')
#  print('read call')

#  csv_files = glob.glob('/root/airflow/dags/indicators_to_regions/Files/sql_files/callls/*.csv')
#  dataframes = []

#  for file in csv_files:
#     df = pd.read_csv(file)
#     dataframes.append(df)

#  calls = pd.concat(dataframes)
#  print('concat call succesfull')

#  calls.reset_index(drop=True, inplace=True)
#  print('read transfer')

#  csv_files = glob.glob('/root/airflow/dags/indicators_to_regions/Files/transfer/*.csv')
#  dataframes = []

#  for file in csv_files:
#     df = pd.read_csv(file)
#     dataframes.append(df)

#  transferfull = pd.concat(dataframes)
#  print('__concat transfer succesfull')

#  transferfull = transferfull.drop_duplicates(subset=['phone', 'datecalls'], keep='last')
#  print('__delete duplicates')
#  transferfull.reset_index(drop=True, inplace=True)
#  print('size transfer: ', transferfull.shape[0])
#  print('size call: ', calls.shape[0])


#  print('request.contact: ', requests.contact.dtype)

#  print('start change types data')
#  calls['datecall'] = calls['datecall'].astype(str)
#  calls['hoursonly'] = calls['hoursonly'].astype(str)
#  calls['hoursonly'] = calls['hoursonly'].apply(lambda x: x.replace('.0', ''))
#  calls['phone'] = calls['phone'].astype(str)
#  calls['phone'] = calls['phone'].apply(lambda x: x.replace('.0', ''))
#  calls['queue_c'] = calls['queue_c'].astype(str)
#  calls['queue_c'] = calls['queue_c'].apply(lambda x: x.replace('.0', ''))


#  transferfull['datecalls'] = transferfull['datecalls'].astype(str)    
#  transferfull['hoursonly'] = transferfull['hoursonly'].astype(str)
#  transferfull['hoursonly'] = transferfull['hoursonly'].apply(lambda x: x.replace('.0', ''))
#  transferfull['phone'] = transferfull['phone'].astype(str)
#  transferfull['phone'] = transferfull['phone'].apply(lambda x: x.replace('.0', ''))
#  transferfull['dialog'] = transferfull['dialog'].astype(str)
#  transferfull['dialog'] = transferfull['dialog'].apply(lambda x: x.replace('.0', ''))
#  transferfull['destination_queue'] = transferfull['destination_queue'].astype(str)
#  transferfull['destination_queue'] = transferfull['destination_queue'].apply(lambda x: x.replace('.0', ''))
#  transferfull['city_c'] = transferfull['city_c'].astype(str)
#  transferfull['city_c'] = transferfull['city_c'].apply(lambda x: x.replace('.0', ''))
#  transferfull['town'] = transferfull['town'].astype(str)
#  transferfull['town'] = transferfull['town'].apply(lambda x: x.replace('.0', ''))
#  requests['contact'] = requests['contact'].astype(str)
#  requests['contact'] = requests['contact'].apply(lambda x: x.replace('.0', ''))

# #  print('call columns: ', calls.columns)
# #  print('transfer columns: ', transferfull.columns)
# #  print(transferfull.head(10))
# #  print(calls.head(10))
# #  print('Переводы полностью прочитаны')
# #  calls.loc[calls['phone'] == '89922404564']
# #  transferfull.loc[transferfull['phone'] == '89922404564']
#  calltransfer = calls.merge(transferfull, how='left', left_on=['phone', 'datecall', 'hoursonly', 'queue_c'],
#                            right_on=['phone', 'datecalls', 'hoursonly', 'dialog'])
# #  print(calltransfer.head(10))
# #  calltransfer.loc[calltransfer['phone'] == '89922404564']
#  print('merge calls and transfer finish')
#  print('size new df ', calltransfer.shape[0])

#  calltransfer['city_c_x'] = calltransfer['city_c_x'].fillna('')
#  calltransfer['city_c_y'] = calltransfer['city_c_y'].fillna('')

#  calltransfer['city_c'] = calltransfer.apply(lambda row: defs.area_defination(row['city_c_y'], row['city_c_x']), axis=1)

#  del calltransfer['city_c_x']
#  del calltransfer['city_c_y']

#  print('defination city_c complete')

#  calltransfer['phone'] = calltransfer['phone'].astype(str)
#  calltransfer['phone'] = calltransfer['phone'].apply(lambda x: x.replace('.0', ''))

# #  call_noduplicates = calltransfer.sort_values(['datecall', 'duration_minutes']).drop_duplicates(subset=['phone', 'userid'], keep='last')

#  print('start merge with request')
#  print('size_request: ', requests.shape[0])

#  requests = requests.merge(calltransfer, how='left', left_on=['contact', 'userid'], right_on=['phone', 'userid'])
 
# #  print(requests.head(10))
# #  requests.loc[requests['contact'] == '89922404564']

#  print('merge with request finished')
#  print('size new df: ', requests.shape[0])
#  path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json'

#  scope = ['https://spreadsheets.google.com/feeds',
#          'https://www.googleapis.com/auth/drive']

#  credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
#  gs = gspread.authorize(credentials)

#  table_name4 = 'Команды/Проекты'

#  work_sheet4 = gs.open(table_name4)
#  sheet4 = work_sheet4.worksheet('Регионы-Города')
#  data4 = sheet4.get_all_values()
#  headers4 = data4.pop(0)
#  city1 = pd.DataFrame(data4, columns=headers4)
#  print('size city1 ', city1.shape[0])
#  city1 = city1.drop_duplicates(subset='Город', keep='first', inplace=False)
#  print('size city1 after drop duplicates', city1.shape[0])

#  city2 = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv', sep=',', encoding='utf-8')

#  city3 = pd.read_csv('/root/airflow/dags/indicators_to_regions/Files/Город.csv', sep=',', encoding='utf-8')

#  city4 = city3.drop_duplicates(subset='town_c', keep='first', inplace=False)

#  city4 = city4.drop(['city_c', 'Город', 'MACRO'], axis=1)
#  city2['city_c'] = city2['city_c'].astype(str)
#  city4['town_c'] = city4['town_c'].astype(str)
#  print('Все файлы для соединения полностью прочитаны')

#  requests['city_c'] = requests['city_c'].astype(str)
#  requests['city_c'] = requests['city_c'].apply(lambda x: x.replace('.0', ''))
#  requests['town'] = requests['town'].astype(str)
#  requests['town'] = requests['town'].apply(lambda x: x.replace('.0', ''))
#  requestcitys = requests.merge(city1, how='left', left_on='city', right_on='Город')
#  print('merge with city1 finished')
#  print('size new df: ', requestcitys.shape[0])

#  requestcitys1 = requestcitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
#  print('merge with city2 finished')
#  print('size new df: ', requestcitys1.shape[0])

#  requestcitys2 = requestcitys1.merge(city4, how='left', left_on='town', right_on='town_c')
#  print('merge with city4 finished')
#  print('size new df: ', requestcitys2.shape[0])


#  requestcitys2.drop(
#     ['contact', 'datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'MACRO', 'Город_x', 'Город_y', 'queue_c', 'hoursonly',
#      'project_c'], axis=1, inplace=True)

#  requestcitys2.rename(
#     columns={'Регион': 'RTK_city', 'Регион ТТК': 'TTK_city', 'Регионы МТС': 'MTS_city', 'Регион Билайн': 'BLN_city',
#              'ТТК Регион_x': 'TTK_city_c', 'РТК Регион_x': 'RTK_city_c', 'МТС Регион_x': 'MTS_city_c',
#              'Билайн Регион_x': 'BLN_city_c', 'ТТК Регион_y': 'TTK_town', 'РТК Регион_y': 'RTK_town',
#              'Билайн Регион_y': 'BLN_town', 'МТС Регион_y': 'MTS_town'}, inplace=True)

#  requestcitys3 = requestcitys2.fillna('')
#  requestcitys3['RTK_region'] = requestcitys3.apply(lambda row: defs.rtk_reg_r(row), axis=1) 
#  requestcitys3['TTK_region'] = requestcitys3.apply(lambda row: defs.ttk_reg_r(row), axis=1)
#  requestcitys3['MTS_region'] = requestcitys3.apply(lambda row: defs.mts_reg_r(row), axis=1)
#  requestcitys3['BLN_region'] = requestcitys3.apply(lambda row: defs.bln_reg_r(row), axis=1)
 
#  requestcitys3 = requestcitys3.fillna('')

#  requestcitys3.drop(
#     ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
#      'RTK_town', 'MTS_town', 'BLN_town'], axis=1, inplace=True)

#  requestcitys3.fillna('').to_csv(f'{path_result}/{file_result_req}', index=False,
#                                 sep=',',
#                                 encoding='utf-8')

#  print('size request df: ', requestcitys3.shape[0])
 

#  print('save request to csv')

#  calltransfer['city_c'] = calltransfer['city_c'].astype(str)
#  calltransfer['city_c'] = calltransfer['city_c'].apply(lambda x: x.replace('.0', ''))
#  calltransfer['town'] = calltransfer['town'].astype(str)
#  calltransfer['town'] = calltransfer['town'].apply(lambda x: x.replace('.0', ''))

#  print('calltransfer: col city_c type', calltransfer['city_c'].dtype, 'col town type', calltransfer['town'].dtype)
#  print('city1: col city_c type', city1['Город'].dtype)
#  print('city2: col town type', city2['city_c'].dtype)
#  print('city4: col town_c type', city4['town_c'].dtype)

#  callscitys = calltransfer.merge(city1, how='left', left_on='city', right_on='Город')
#  print('merge with city1 finished')
#  print('size new df: ', callscitys.shape[0])
#  callscitys1 = callscitys.merge(city2, how='left', left_on='city_c', right_on='city_c')
#  print('merge with city2 finished')
#  print('size new df: ', callscitys1.shape[0])
#  callscitys2 = callscitys1.merge(city4, how='left', left_on='town', right_on='town_c')
#  print('merge with city4 finished')
#  print('size new df: ', callscitys2.shape[0])
#  callscitys2.drop(['datecalls', 'town_c_y', 'Область_x', 'town_c_x', 'MACRO', 'Город_x', 'Город_y', 'queue_c'], axis=1,
#                  inplace=True)

#  callscitys2.rename(
#     columns={'Регион': 'RTK_city', 'Регион ТТК': 'TTK_city', 'Регионы МТС': 'MTS_city', 'Регион Билайн': 'BLN_city',
#              'ТТК Регион_x': 'TTK_city_c', 'РТК Регион_x': 'RTK_city_c', 'МТС Регион_x': 'MTS_city_c',
#              'Билайн Регион_x': 'BLN_city_c', 'ТТК Регион_y': 'TTK_town', 'РТК Регион_y': 'RTK_town',
#              'Билайн Регион_y': 'BLN_town', 'МТС Регион_y': 'MTS_town'}, inplace=True)

#  callscitys2 = callscitys2.fillna('')
#  callscitys2['RTK_region'] = callscitys2.apply(lambda row: defs.rtk_reg(row), axis=1)
#  print('РТК регион')
#  callscitys2['TTK_region'] = callscitys2.apply(lambda row: defs.ttk_reg(row), axis=1)
#  print('ТТК регион')
#  callscitys2['MTS_region'] = callscitys2.apply(lambda row: defs.mts_reg(row), axis=1)
#  print('МТС регион')
#  callscitys2['BLN_region'] = callscitys2.apply(lambda row: defs.bln_reg(row), axis=1)
#  print('Билайн регион')
 
#  callscitys2 = callscitys2.fillna('')

#  callscitys2.drop(
#     ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
#      'RTK_town', 'MTS_town', 'BLN_town'], axis=1, inplace=True)

#  callscitys2 = callscitys2.drop_duplicates().fillna('')
#  print('save callcitys to csv')

#  callscitys2.to_csv(f'{path_result}/{file_result}',sep=',', index=False)

