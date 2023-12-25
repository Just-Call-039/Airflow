def short_editer(path_to_files, calls,calls_out,robot, path_result,file_result):
 import pandas as pd
 import pymysql
 import gspread 
 from oauth2client.service_account import ServiceAccountCredentials
 import datetime
 import os
 import glob

 print('Начинаем обработку данных')
 calls = pd.read_csv(f'{path_to_files}/{calls}')
 calls_out = pd.read_csv(f'{path_to_files}/{calls_out}')
 robot = pd.read_csv(f'{path_to_files}/{robot}')
 print('Все файлы прочитаны')

 robot['hours'] = robot['hours'].astype(str)
 robot['phone'] = robot['phone'].astype(str)
 calls['hours'] = calls['hours'].astype(str)
 calls['phone'] = calls['phone'].astype(str)
 calls_out['phone'] = calls_out['phone'].astype(str)
 calls['hours'] = calls['hours'].apply(lambda x: x.replace('.0',''))
 calls['phone'] = calls['phone'].apply(lambda x: x.replace('.0',''))
 robot['hours'] = robot['hours'].apply(lambda x: x.replace('.0',''))
 calls['phone'] = calls['phone'].apply(lambda x: x.replace('.0',''))
 calls_out['phone'] = calls_out['phone'].apply(lambda x: x.replace('.0',''))


 short_callsv = robot.merge(calls, left_on = ['phone','calldate','hours'], 
                          right_on = ['phone','call_date','hours'], how = 'left').fillna('')
 
 
 short_calls2 = short_callsv.merge(calls_out, left_on = ['phone','user_call'], 
                          right_on = ['phone','user_call'], how = 'left').fillna('')
 csv_files = glob.glob('/root/airflow/dags/project_defenition/projects/steps/*.csv')
 dataframes  = []

 for file in csv_files:
    df = pd.read_csv(file)
    dataframes.append(df)

 steps = pd.concat(dataframes)
 short_calls2['last_step'] = short_calls2['last_step'].astype(str)
 short_calls2['set_queue'] = short_calls2['set_queue'].astype(str)
 short_calls2['calldate'] = pd.to_datetime(short_calls2['calldate'])
 steps['step'] = steps['step'].astype(str)
 steps['ochered'] = steps['ochered'].astype(str)
 steps['date'] = pd.to_datetime(steps['date'])
 short_calls2 = short_calls2.merge(steps, left_on = ['last_step','set_queue','calldate'], 
                          right_on = ['step','ochered','date'], how = 'left').fillna('')
 short_calls2['perevod'] = 0
 short_calls2.loc[(short_calls2['last_step'] == short_calls2['step']) & 
       (short_calls2['set_queue'] == short_calls2['ochered']) & 
       (short_calls2['date'] == short_calls2['calldate']), 'perevod'] = 1
 
 short_calls = short_calls2[['phone','calldate','hours','set_queue','perevod','talk','meeting',
                            'name','queue','user_call','call_sec','short_calls','completed_c','result_call_c','countout','otkaz_c']]
 
 short_calls = short_calls.groupby(['calldate',	
                                    'hours',
                'set_queue',
                'perevod',
                'talk',
                'meeting',
                'name',
                'queue',
                'user_call',
                'call_sec',
                'short_calls',
                'completed_c',
                'result_call_c',
                'countout','otkaz_c'],as_index=False, dropna=False).agg({'phone':'count'}).rename(columns={'phone': 'calls'})
 
 
 print('Записывается в файл')
 short_calls.to_csv(f'{path_result}/{file_result}',sep=';', index=False)