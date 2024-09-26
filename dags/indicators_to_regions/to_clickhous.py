import pandas as pd
from clickhouse_driver import Client
import MySQLdb
import datetime
import time
import os
import glob
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from indicators_to_regions import defs, download_googlesheet


def to_click(path_to_files, csv_city, path_to_request, path_to_call, path_to_transfer, path_to_user, path_to_decoding, path_to_workhour, path_to_workprevios):
 
 request = pd.read_csv(path_to_request).fillna('')
#  ,  sep=',', encoding='utf-8'
 print('___request download') 
 print('__size request ', request.shape[0])
 print('___request columns ', request.columns) 


 print('___start proccess')
# Загружаем датасеты
 
# Звонки
 print('___start read call')
 call = defs.download_files(path_to_call)
 
 print('___download call done')
 print('call ', call.shape[0])
 
# Переводы
 print('___start read transfer')
 transfer = defs.download_files(path_to_transfer)
 
 print('___download transfer done')
 print('transfer ', transfer.shape[0])

# Убираем нули, преобразуем типы данных

 col_list =['hoursonly', 'city_c', 'queue_c', 'duration_minutes', 'phone']
 call[col_list] = call[col_list].fillna('0').astype(str)
 defs.del_point_zero(call, col_list)

 call['datecall'] = call['datecall'].astype("datetime64[ns]").dt.to_period("D").astype(str)
 call = call.fillna('0')

#  Убираем дубликаты в переводах, чтобы не вылазили дубликаты при слиянии со звонками

 transfer = transfer.drop_duplicates(subset=['phone', 'datecalls'], keep='last')
 print('__delete duplicates')
 transfer.reset_index(drop=True, inplace=True)
 print('___size transfer: ', transfer.shape[0])
 print('___size call: ', call.shape[0])

# Поменяем типы данных для мерджа
 print('___start change types data')

 call['queue_c'] = call['queue_c'].fillna('0')
 call['city_c'] = call['city_c'].fillna('0')
 
 transfer['datecall'] = transfer['datecalls'].astype(str)

 col_list = ['hoursonly', 'phone']
 transfer[col_list] = transfer[col_list].astype(str)
 defs.del_point_zero(transfer, col_list)

 transfer['dialog'] = transfer['dialog'].fillna(0).astype(str)
 transfer['destination_queue'] = transfer['destination_queue'].fillna(0).astype(str)
 transfer['city_c'] = transfer['city_c'].fillna(0).astype(int).astype(str)
 transfer['town'] = transfer['town'].fillna(0).astype(int).astype(str)
 
 print('___datatypes changed')

# Мерджим датасет звонки с переводами
 calltransfer = call.merge(transfer[['phone', 'datecall', 'hoursonly', 'dialog', 'city_c', 'city', 'town', 'destination_queue']],
                               how='left', on=['phone', 'datecall', 'hoursonly'])
 print('___merge calls and transfer finish')
 print('___size new df ', calltransfer.shape[0])

#  Определим поле код города после слияния
 calltransfer['city_c_x'] = calltransfer['city_c_x'].fillna('0').astype('int64')
 calltransfer['city_c_y'] = calltransfer['city_c_y'].fillna('0').astype('int64')

 calltransfer['city_c'] = calltransfer.apply(lambda row: defs.fill_nan(row['city_c_x'], row['city_c_y']), axis=1)

 del calltransfer['city_c_x']
 del calltransfer['city_c_y']
 print('___defination city_c complete')

 calltransfer['city'] = calltransfer['city'].fillna('0')

# Определим поля область и город на основе двух датасетов:
# Если значение 0, но в этот день был еще звонок на этот номер и город называли - то берем значение из другого звонка

 calltransfer['town'] = calltransfer['town'].fillna('0').astype('int64')

 calltransfer['city_guess'] = calltransfer.sort_values(['datecall', 'phone', 'city_c'], ascending=False).\
                                          groupby(['datecall', 'phone'])['city_c'].cummax()
 calltransfer['town_guess'] = calltransfer.sort_values(['datecall', 'phone', 'town'], ascending=False).\
                                          groupby(['datecall', 'phone'])['town'].cummax()
 print('___create columns guess town and city')

 calltransfer['city_c'] = calltransfer.apply(lambda row: defs.fill_nan(row['city_c'], row['city_guess']), axis = 1).astype(str)
 calltransfer['town'] = calltransfer.apply(lambda row: defs.fill_nan(row['town'], row['town_guess']), axis = 1).astype(str)

 print('___city_c and town was definated')

 # Загружаем датасеты-справочники с городами

 city_region = download_googlesheet.download_gs('Команды/Проекты', 'Регионы-Города')
 print('___size city_region ', city_region.shape[0])
 print('___columns city_region ', city_region.columns)

#  Удаляем дубликаты
 city_region = city_region.drop_duplicates(subset='Город', keep='first', inplace=False)
 print('___size city_region after drop duplicates', city_region.shape[0])

 dict_city = pd.read_csv(f'{path_to_files}{csv_city}')
 del dict_city['Название из JSON карты']

# Мерджим города с регионами, чтобы подтянуть в города значения из обоих справочников
# Из справочника город, будем подтягивать названия городов по city_c в наш датасет со звонками
 dict_city['Город'] = dict_city['Город'].apply(defs.find_letter)
 dict_city = dict_city[['city_c', 'town_c', 'Город', 'Область', 'ТТК Регион', 'РТК Регион', 'МТС Регион', 'Билайн Регион']].merge(
                            city_region, how = 'left', on = 'Город')
 
#  Заполняеи поля с регионами проектов
 dict_city = dict_city.fillna('0')
 dict_city['ТТК Регион'] = dict_city.apply(lambda row: defs.fill_nan(row['ТТК Регион'], row['Регион ТТК']), axis = 1)
 dict_city['РТК Регион'] = dict_city.apply(lambda row: defs.fill_nan(row['РТК Регион'], row['Регион']), axis = 1)
 dict_city['МТС Регион'] = dict_city.apply(lambda row: defs.fill_nan(row['МТС Регион'], row['Регионы МТС']), axis = 1)
 dict_city['Билайн Регион'] = dict_city.apply(lambda row: defs.fill_nan(row['Билайн Регион'], row['Регион Билайн']), axis = 1)
 
 del dict_city['Регион ТТК']
 del dict_city['Регион']
 del dict_city['Регионы МТС']
 del dict_city['Регион Билайн']

# Мерджим регионы с городами, чтобы подтянуть данные 
# Из этого датасета будем тянуть данные к какому региону по проектам относится звонок
 city_region = city_region.merge(dict_city[['Город', 'ТТК Регион', 'РТК Регион', 'МТС Регион', 'Билайн Регион']].drop_duplicates('Город'), how = 'left', on = 'Город')

#  Заполняеи поля с регионами проектов
 city_region = city_region.fillna('0')
 city_region['Регион ТТК'] = city_region.apply(lambda row: defs.fill_nan(row['Регион ТТК'], row['ТТК Регион']), axis = 1)
 city_region['Регион'] = city_region.apply(lambda row: defs.fill_nan(row['Регион'], row['РТК Регион']), axis = 1)
 city_region['Регионы МТС'] = city_region.apply(lambda row: defs.fill_nan(row['Регионы МТС'], row['МТС Регион']), axis = 1)
 city_region['Регион Билайн'] = city_region.apply(lambda row: defs.fill_nan(row['Регион Билайн'], row['Билайн Регион']), axis = 1)

 del city_region['ТТК Регион']
 del city_region['РТК Регион']
 del city_region['МТС Регион']
 del city_region['Билайн Регион']

# Cоздаем справочник, откуда будем в дальнейшем подтягивать области в датасет звонки
 dict_town = dict_city.drop_duplicates(subset='town_c', keep='first', inplace=False)

 dict_town = dict_town.drop(['city_c', 'Город'], axis=1)
 dict_city['city_c'] = dict_city['city_c'].astype(str)
 dict_town['town_c'] = dict_town['town_c'].astype(str)
 print('___all dict city done')

#  col_list = ['city_c', 'town']
#  calltransfer[col_list] = calltransfer[col_list].fillna(0).astype(str)
#  defs.del_point_zero(calltransfer, col_list)

 print('__calltransfer: col city_c type', calltransfer['city_c'].dtype, 'col town type', calltransfer['town'].dtype)
 print('__city_region: col city_c type', city_region['Город'].dtype)
 print('__dict_city: col town type', dict_city['city_c'].dtype)
 print('__dict_town: col town_c type', dict_town['town_c'].dtype)

# Мерджим датасет со справочниками города и регионы

 callregion = calltransfer.merge(city_region, how='left', left_on='city', right_on='Город')
 print('__merge with city_region finished')
 print('__size new df: ', callregion.shape[0])
 calldictcity = callregion.merge(dict_city, how='left', left_on='city_c', right_on='city_c')
 print('__merge with dict_city finished')
 print('__size new df: ', calldictcity.shape[0])
 calltown = calldictcity.merge(dict_town, how='left', left_on='town', right_on='town_c')
 print('__merge with dict_town finished')
 print('__size new df: ', calltown.shape[0])

# Удаляем ненужные поля
 calltown.drop(['town_c_y', 'Область_x', 'town_c_x', 'Город_x', 'Город_y'], axis=1,
                 inplace=True)
 
# Переименовываем столбцы
 calltown.rename(
    columns={'Регион': 'RTK_city', 'Регион ТТК': 'TTK_city', 'Регионы МТС': 'MTS_city', 'Регион Билайн': 'BLN_city',
             'ТТК Регион_x': 'TTK_city_c', 'РТК Регион_x': 'RTK_city_c', 'МТС Регион_x': 'MTS_city_c',
             'Билайн Регион_x': 'BLN_city_c', 'ТТК Регион_y': 'TTK_town', 'РТК Регион_y': 'RTK_town',
             'Билайн Регион_y': 'BLN_town', 'МТС Регион_y': 'MTS_town'}, inplace=True)

 calltown = calltown.fillna('0')
 print('___size calltown df: ', calltown.shape[0])

# Опеределим регионы проектов
 calltown['RTK_region'] = calltown.apply(lambda row: defs.region_defination(row['RTK_city'],
                                                                                 row['RTK_city_c'],
                                                                                 row['RTK_town']), axis=1)
 print('__РТК region complete')
 calltown['TTK_region'] = calltown.apply(lambda row: defs.region_defination(row['TTK_city'], 
                                                                                 row['TTK_city_c'],
                                                                                 row['TTK_town']), axis=1) 
 print('__ТТК region complete')
 calltown['MTS_region'] = calltown.apply(lambda row: defs.region_defination(row['MTS_city'], 
                                                                                 row['MTS_city_c'],
                                                                                 row['MTS_town']), axis=1) 
 print('__МТС region complete')
 calltown['BLN_region'] = calltown.apply(lambda row: defs.region_defination(row['BLN_city'], 
                                                                                 row['BLN_city_c'],
                                                                                 row['BLN_town']), axis=1) 
 print('__BLN region complete')

# Удаляем ненужные столбцы
 calltown.drop(
    ['RTK_city', 'TTK_city', 'MTS_city', 'BLN_city', 'TTK_city_c', 'RTK_city_c', 'BLN_city_c', 'MTS_city_c', 'TTK_town',
     'RTK_town', 'MTS_town', 'BLN_town', 'city_guess', 'town_guess'], axis=1, inplace=True)

 calltown = calltown.fillna('0')
 print('___size calltown df: ', calltown.shape[0])

#  Подтянем значения причин отказов из справочника

 df_decoding_otkaz = pd.read_excel(path_to_decoding, sheet_name = 'Лист1')
 df_decoding_otkaz = df_decoding_otkaz.rename(columns={'name':'otkaz_c'})
 df_decoding_otkaz['otkaz_c'] = df_decoding_otkaz['otkaz_c'].astype(str)
 print('___decoding', df_decoding_otkaz[df_decoding_otkaz['otkaz_c'] == '0'].shape[0])
 print('___call', calltown[calltown['otkaz_c'] == '0'].shape[0])


 callotkaz =  calltown.merge(df_decoding_otkaz, on = 'otkaz_c', how = 'left').fillna('')
 print('___call', callotkaz['name_ru'].unique())

 print('___size df callotkaz ', callotkaz.shape[0])

#  Сгруппируем датасет, чтобы уменьшить кол-во строк

 callotkaz = callotkaz[['datecall', 'userid', 'dialog', 'result_call_c', 
       'project_c', 'phone', 'town', 'destination_queue',
       'city_c', 'city', 'Область_y', 'RTK_region', 'TTK_region', 'MTS_region',
       'BLN_region', 'name_ru', 'otkaz_c']].fillna('0')

 call_group = callotkaz.groupby(['datecall', 'dialog', 'result_call_c', 'userid',
       'project_c', 'phone', 'town', 'destination_queue',
       'city_c', 'city', 'Область_y', 'RTK_region', 'TTK_region', 'MTS_region',
       'BLN_region', 'name_ru'], dropna=False).agg({'otkaz_c': ['count']}).\
                             reset_index()
    
 call_group.columns = call_group.columns.droplevel(1)
  
 print('df size after group ', call_group.shape[0])
 print('df count call after group ', call_group[call_group['phone'] != '0']['otkaz_c'].sum())

#  Удалим строки с нулевым значением телефона
 call_group = call_group[call_group['phone'] != '0']

 print('df size after group ', call_group.shape[0])
 print('df count call after group ', call_group['otkaz_c'].sum())

# Объединяем датафрейм с пользователями, чтобы достать поля супервайзер и фио оператора
 print('merge df & users')
 user = pd.read_csv(path_to_user,  sep=',', encoding='utf-8').fillna('')
 call_user = call_group.merge(user, left_on = 'userid', right_on = 'id', how = 'left').fillna('0')

 print('size call_group ', call_user.shape[0])

# Загрузим лиды и проекты

 lids = download_googlesheet.download_gs('Команды/Проекты', 'Лиды')
 print('size lids ', lids.shape[0])

 jc = download_googlesheet.download_gs('Команды/Проекты', 'JC')
 print('size jc ', jc.shape[0])


# merge с лидами
 print('___merge call_group & lids')
 call_lids=  call_user.merge(lids[['Проект','СВ CRM', 'МРФ']], left_on = 'supervisor', right_on = 'СВ CRM', how = 'left').fillna('')
 print('__size df ', call_lids.shape[0])

# merge с проектами
 print('___merge call_group & jc')
 call_jc =  call_lids.merge(jc[['Проект','CRM СВ']], left_on = 'supervisor', right_on = 'CRM СВ', how = 'left').fillna('')
 print('___size df ', call_jc.shape[0])

# Определим поле Проект
 call_jc['Проект'] = call_jc.apply(lambda row: defs.fill_nan(row['Проект_x'], row['Проект_y']), axis=1)

# Загружаем заявки

 request = pd.read_csv(path_to_request).fillna('')

 print('___request download') 
 print('__size request ', request.shape[0])
 print('___request columns ', request.columns) 

#  Убирем лишний ноль из телефона
 request['my_phone_work'] = request['my_phone_work'].astype(str)
 defs.del_point_zero(request, ['my_phone_work'])

# Меняем типы данных для дальнейшего merge
 request['request_date'] = pd.to_datetime(request['request_date'])
 request = request[(request['request_date'] >= '2024-02-01') & (request['request_date'] <= pd.to_datetime('today'))]

 print('___size request ', request.shape[0])
 request['request_date']=request['request_date'].fillna('').astype('str')
 request['my_phone_work']=request['my_phone_work'].fillna('').astype('str')

#  Переименуем столбцы, чтобы не дублировались
 request = request[['request_date', 'user', 'status', 'district_c','my_phone_work']].\
                                                            rename(columns = {'status' : 'status_request',
                                                                              'district_c' : 'queue_request'                                                           
                                                                             })

# Создадим столбец NN. Будем мерджить по 1 - самый первый звонок за день на этот номер
 call_jc = call_jc.sort_values(['datecall', 'userid', 'phone'])
 call_jc['NN'] = call_jc.groupby(['datecall', 'userid', 'phone']).cumcount() + 1
 call_jc['datecall']=call_jc['datecall'].fillna('').astype('str')

# Так же для заявок. Будем мерджить по 1 - самая первая заявка за день на этот номер
# Все заявки попадут в датасет, но не продублируют звонки
 request = request.sort_values(['request_date', 'user', 'my_phone_work'])    
 request['NN'] = request.groupby(['request_date', 'user', 'my_phone_work']).cumcount() + 1

# Объединяем датафрейм с запросами
 print('start merge df_request')
 df =  call_jc.merge(request, left_on = ['phone', 'userid', 'datecall', 'NN'],
                                right_on = ['my_phone_work', 'user', 'request_date', 'NN'], how = 'outer')
 
 print('размер датасета  ', df.shape[0])
 
# Вернем столбцам с датами тип дата 
 df['datecall'] = pd.to_datetime(df['datecall'])
 df['request_date'] = pd.to_datetime(df['request_date'])
 df = df.fillna('0')

 print('кол-во звонков', df['otkaz_c'].astype('int64').sum())
 print('кол-во заявок в датасете', df[df['request_date'] != '0'].shape[0])

# Выгружаем датафрейм с рабочими часами
 workhour_current = pd.read_csv(path_to_workhour, sep = ';')
 workhour_previos = defs.download_files(path_to_workprevios)
 workhour = pd.concat([workhour_current, workhour_previos], ignore_index = True, axis=0)

 print('size work', workhour.shape[0])


# Создадим столбец с общим временем
    
 def total_sec(row):
     col_list = ['talk_inbound', 'talk_outbound', 'ozhidanie', 'obrabotka', 'training', 'nastavnik', 'sobranie', \
                                   'problems', 'obuchenie', 'dorabotka']
     result = 0
     for col in col_list:
             result += row[col]
     return result
    
 workhour['total_sec'] = workhour.apply(lambda row: total_sec(row), axis=1)

# Преобразуем и переименуем данные для дальшнейшего мерджа
 work = work[['id_user', 'date', 'total_sec']].rename(columns={'id_user': 'userid', 'date' : 'datecall'})
 df.loc[df['datecall'] == '0','datecall'] = ''
 df['datecall'] = df['datecall'].dt.to_period('D').astype('str').fillna('')

# Смерджим датасет с рабочими часами 
 df = df.merge(work, how = 'left', on = ['userid', 'datecall'])
 print('size df', df.shape[0])
 print(df['total_sec'].unique())

# Переименуем и оставим только нужные столбцы

 df = df[['datecall',
         'dialog', 
         'userid', 
         'phone',
         'town', 
         'destination_queue', 
         'city_c', 
         'city', 
         'Область_y',
         'RTK_region', 
         'TTK_region', 
         'MTS_region', 
         'BLN_region', 
         'name_ru',
         'fio', 
         'supervisor',
         'total_sec',
         'otkaz_c', 
         'Проект',
         'request_date',
         'user',
         'my_phone_work',
         'status_request', 
         'queue_request' 
         ]].rename(columns={'datecall' : 'Дата',
                           'dialog' : 'Набирающая очередь', 
                           'userid' : 'userID', 
                           'phone' : 'phone',
                           'town' : 'town_c', 
                           'destination_queue' : 'Принимающая очередь', 
                           'city_c' : 'city_c', 
                           'city' : 'Город', 
                           'Область_y' : 'Область',
                           'RTK_region' : 'РТК регион', 
                           'TTK_region' : 'ТТК регион', 
                           'MTS_region' : 'МТС регион', 
                           'BLN_region' : 'Билайн регион', 
                           'name_ru' : 'Причина отказа', 
                           'fio' : 'Оператор', 
                           'supervisor' : 'Супервайзер',
                           'total_sec' : 'work_hour',
                           'otkaz_c' : 'count_call',
                           'request_date' : 'date_request',
                           'user' : 'user_request',
                           'my_phone_work' : 'request_phone',
                           'status_request' : 'status_request', 
                           'queue_request' :'queue_request' })

# Подтянем в датасет значения регионов по пользователю
 
 lids = lids.rename(columns={'СВ CRM': 'Супервайзер', 'Проект' : 'Проект_супервайзер'})
 df = df.merge(lids[['Супервайзер', 'Проект_супервайзер', 'МРФ']], how = 'left', on = 'Супервайзер')

 df.loc[df['date_request'] == '0','date_request'] = ''
 df.loc[df['Дата'] == '0', 'Дата'] = ''

 # Отправляем в clickhous

 print('Подключаемся к clickhouse')

 # Достаем host, user & password
 dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
 if dest:
            with open(dest) as file:
                for now in file:
                    now = now.strip().split('=')
                    first, second = now[0].strip(), now[1].strip()
                    if first == 'host':
                        host = second
                    elif first == 'user':
                        user = second
                    elif first == 'password':
                        password = second

 # Записываем новый данные в таблицу usermetric_call
 client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})
  
# # Создаем таблицу usermetric_call
 print('drop table call')
 client.execute('drop table suitecrm_robot_ch.indicators_to_region')
 client = Client(host=host, port='9000', user=user, password=password,
                database='suitecrm_robot_ch', settings={'use_numpy': True})

# Создаем таблицу userrefusal_call_previos
 print('Create table call')
 sql_create = '''create table suitecrm_robot_ch.indicators_to_region
    (
    Дата                        Date,
    Набирающая очередь          String,
    userID                      String,            
    phone                       String,
    town_c                      String,
    Принимающая очередь         String,
    city_c                      String,
    Город                       String,
    Область                     String,
    РТК регион                  String,
    ТТК регион                  String,
    МТС регион                  String,
    Билайн регион               String,
    Причина отказа              String,
    Оператор                    String,
    Супервайзер                 String,
    work_hour                   Int64,
    count_call                  Int64,
    date_request                Date,
    request_phone               String,
    user_request                String,
    status_request              String,
    queue_request               String,
    Проект_супервайзер          String,
    МРФ                         String
    )
    engine = MergeTree ORDER BY CallDate;'''
 client.execute(sql_create)

# Записываем новый данные в таблицу usermetric_call
 client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

 client.insert_dataframe('INSERT INTO suitecrm_robot_ch.indicators_to_region VALUES', df)







































 
