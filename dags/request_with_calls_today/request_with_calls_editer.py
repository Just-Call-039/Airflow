def request_editer(path_to_files, request, path_result, file_result):
   import pandas as pd
   import pymysql
   import datetime
   import os
   import fsp.def_project_definition as def_project_definition
   from clickhouse_driver import Client
   import datetime
   import os
   import glob

   csv_files = glob.glob('/root/airflow/dags/previous_month/Files/calls_with_request/*.csv')
   dataframes = []

   for file in csv_files:
      df = pd.read_csv(file)
      dataframes.append(df)

   Callreqfull = pd.concat(dataframes)

   Callreqfull.reset_index(drop=True, inplace=True)

   request_now = pd.read_csv(f'{path_to_files}/{request}')
   print('request ', request_now[request_now['user'] == 'e9abfebd-cb27-9a24-61bc-6786646edd55'])

   request_now["my_phone_work"] = request_now['my_phone_work'].astype(object)
   request_now["my_phone_work"] = request_now['my_phone_work'].astype(str)

   Callreqfull["phone_number"] = Callreqfull['phone_number'].astype(object)
   Callreqfull["phone_number"] = Callreqfull['phone_number'].astype(str)

   request_now["user"] = request_now['user'].astype(object)
   request_now["user"] = request_now['user'].astype(str) 
   Callreqfull["call_date"] = Callreqfull['call_date'].astype(str)
   request_now["request_date"] = request_now['request_date'].astype(str) 

 

   Callreqfull["assigned_user_id"] = Callreqfull['assigned_user_id'].astype(object)
   Callreqfull["assigned_user_id"] = Callreqfull['assigned_user_id'].astype(str)
   print('call ', Callreqfull[Callreqfull['assigned_user_id'] == 'e9abfebd-cb27-9a24-61bc-6786646edd55'])

   print(f'Заявки {request_now.shape[0]}')

   Requests = request_now.merge(Callreqfull, how = 'left', left_on=['my_phone_work','user'], right_on=['phone_number','assigned_user_id'])
   print('union ', Requests[Requests['user'] == 'e9abfebd-cb27-9a24-61bc-6786646edd55'])
   print(f'Заявки после соединиения {Requests.shape[0]}')
   Requests['request_hour'] = Requests['request_hour'].astype('str').apply(lambda x: x.replace('.0',''))
   Requests = Requests[['project','request_date','request_hour',
                      'user','super','status','last_queue_c','district_c',
                      'id_call','call_date','result_call_c','city',
                      'num','queue','assigned_user_id','my_phone_work']]
 
   Requests.to_csv(f'{path_result}/{file_result}',sep=',', index=False)

#    req_click = Requests.copy()
#    req_click = req_click[['project','request_date','request_hour',
#                       'user','super','status','last_queue_c','district_c',
#                       'city','queue','my_phone_work']]
#    req_click['request_hour'] = req_click['request_hour'].astype('str').apply(lambda x: x.replace('.0',''))
#    req_click['city'] = req_click['city'].astype('str').apply(lambda x: x.replace('.0',''))
#    req_click['last_queue_c'] = req_click['last_queue_c'].astype('str').apply(lambda x: x.replace('.0',''))
#    req_click['queue'] = req_click['queue'].astype('str').apply(lambda x: x.replace('.0',''))

#    stat = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/Статусы заявок.csv',  sep=',', encoding='utf-8')

#    req_click = req_click.merge(stat, how = 'left', on='status')
#    print(f'Заявки после соединиения {req_click.shape[0]}')

   
#    req_click = req_click.astype('str')
#    req_click['request_date'] = pd.to_datetime(req_click['request_date'])
 

#    print('Подключаемся к clickhouse')
#    dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
#    if dest:
#       with open(dest) as file:
#             for now in file:
#                now = now.strip().split('=')
#                first, second = now[0].strip(), now[1].strip()
#                if first == 'host':
#                             host = second
#                elif first == 'user':
#                             user = second
#                elif first == 'password':
#                             password = second
#         # return host, user, password


#    client = Client(host=host, port='9000', user=user, password=password,
#                     database='suitecrm_robot_ch', settings={'use_numpy': True})


#    print('Удаляем таблицу')
#    client.execute('drop table suitecrm_robot_ch.jc_meeting_module')


#    client = Client(host=host, port='9000', user=user, password=password,
#                     database='suitecrm_robot_ch', settings={'use_numpy': True})

#    print('Создаем таблицу')
#    sql_create = '''create table jc_meeting_module
# (
#     project       String,
#     request_date  Date,
#     request_hour  Int64,
#     user          String,
#     super         String,
#     status        String,
#     last_queue_c  String,
#     district_c    String,
#     city          String,
#     queue         String,
#     my_phone_work String,
#     req_status    String
# )
#     engine = MergeTree
#         ORDER BY request_date'''
#    client.execute(sql_create)

#    client = Client(host=host, port='9000', user=user, password=password,
#                     database='suitecrm_robot_ch', settings={'use_numpy': True})
   
   
#    print('Отправляем запрос c заявками')
#    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.jc_meeting_module VALUES', req_click)


#    print('Удаляем таблицу')
#    client.execute('drop table suitecrm_robot_ch.users_meet')


#    client = Client(host=host, port='9000', user=user, password=password,
#                     database='suitecrm_robot_ch', settings={'use_numpy': True})

#    print('Создаем таблицу')
#    sql_create = '''create table suitecrm_robot_ch.users_meet
# (
#     id    String,
#     fio  String,
#     team        String,
#     supervisor String
# )
#     engine = MergeTree ORDER BY id
# '''
#    client.execute(sql_create)
#    users = pd.read_csv('/root/airflow/dags/request_with_calls_today/Files/users.csv',  sep=',', encoding='utf-8')
#    users = users[['id','fio','team','supervisor']].astype('str')

#    client = Client(host=host, port='9000', user=user, password=password,
#                     database='suitecrm_robot_ch', settings={'use_numpy': True})
   
   
#    print('Отправляем запрос c заявками')
#    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.users_meet VALUES', users)

