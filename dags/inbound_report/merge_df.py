import pandas as pd
import logging
from datetime import datetime
import glob
import os

from inbound_report import defs
from commons_liza import google_sheet

logging.basicConfig(level=logging.INFO)












def union_with_inbound(truba_path, inbound_path, type_dict, result_path):

    # Загружаем все inbound из папки с архивом за последние 30 дней

    i = 0 

    for file_csv in os.listdir(inbound_path):

        df = pd.read_csv(f'{inbound_path}{file_csv}', dtype = type_dict)

        if i == 0:
            inbound_df = df.copy()
            i += 1
        else:
    
            inbound_df = pd.concat([inbound_df, df])
        
    truba_df = pd.read_csv(truba_path, dtype = type_dict)

    # Проверяем соответствуют ли даты в двух датафреймах, который потом будем мерджить

    logging.info(f'inbound min date: {inbound_df.date_t.min()} and max date {inbound_df.date_t.max()}')
    logging.info(f'truba_df min date: {truba_df.date_t.min()} and max date {truba_df.date_t.max()}')
    
    # Подсчитываем количество звонков в день с каждого номера

    inbound_df['count'] = 1
    inbound_df = inbound_df.fillna('0').groupby(['phone', 'date_t', 'queue_i', 'exit_point'],
                                    as_index = False)['count'].sum()
    
    # Создаем столбцы час и минута для последующего джойна
    defs.create_times_columns(inbound_df, 'date_t')
    defs.create_times_columns(truba_df, 'date_t')

    # Создаем столбец с счетчиком порядкового номера звонка с конкретного контакта ха каждый день (для джойна)
    truba_df['NN'] = truba_df.sort_values(['phone', 'date_t']).groupby(['phone', 'hour', 'minute']).cumcount() + 1

    # Удаляем столбец с секундами (они могут отличаться в таблицах inbound и на трубе)
    del inbound_df['second']

    # Создаем столбцы с датами без времени

    inbound_df['date'] = inbound_df['date_t'].dt.date
    truba_df['date'] = truba_df['date_t'].dt.date

    #  В таблице inbound так же создаем столбец с порядковым номером звонка
    inbound_df['NN'] = inbound_df.sort_values(['phone', 'date_t']).groupby(['phone', 'hour', 'minute']).cumcount() + 1
    
    # Удаляем столбец с датой в inbound
    del inbound_df['date_t']

    union_df = truba_df.merge(inbound_df, how = 'left', on = ['date', 'hour', 'minute', 'phone', 'NN']).fillna('0')
    logging.info(f'exit_point {union_df.exit_point.unique()}')


    # Меняем значения минут и часов для тех звонков, которые совершили в пограничное время 56-59 секунд

    union_df['minute'] = union_df.apply(lambda row: defs.set_minute(row['minute'], row['second']), axis = 1)
    union_df['hour'] = union_df.apply(lambda row: defs.set_hour(row['hour'], row['minute']), axis = 1)
    union_df['minute'] = union_df['minute'].apply(defs.set_minute_60)

    # Переопределяем столбец NN
    union_df['NN'] = union_df.sort_values(['phone', 'date_t']).groupby(['phone', 'hour', 'minute']).cumcount() + 1

    # Джойним датафреймы
    union_df = union_df.merge(inbound_df, how = 'left', on = ['date', 'hour', 'minute', 'phone', 'NN']).fillna('0')

    # Определяем столбцы, которые задублировались во время повторного джойна
    col_list = ['queue_i', 'exit_point', 'count']

    union_df = defs.column_choose(col_list, union_df)
    
    logging.info(f'union_df size after second join: {union_df.shape[0]}')
      
    union_df['count'] = 1
    union_df['daily_count'] = union_df.groupby('phone')['count'].transform('sum')
    union_df['billsec_t'] = union_df['billsec_t'].astype('int64')
    union_df['spam'] = union_df['daily_count'].apply(lambda x: 1 if x > 3 else 0) 
    
    
    union_df = union_df[['date_t', 'uniqueid', 'phone', 'billsec_t', 'lastapp_t', 'hour', \
                         'date', 'queue_i', 'exit_point', 'project', 'daily_count', 'count', 'spam', 'did']]
                  
    union_df.to_csv(f'{result_path}', index = False)  











def union_robot_df(start_path, robot_path, step_path, result_path, type_dict, date_i, numdays):

    start_df = pd.read_csv(start_path, dtype = type_dict)
    logging.info(f'start size: {start_df.shape[0]}')
    

    robot_df = pd.read_csv(robot_path, dtype = type_dict)
    logging.info(f'robot_df size: {robot_df.shape[0]}')
    
    defs.create_times_columns(robot_df, 'date')
    defs.create_times_columns(start_df, 'date_t')

    start_df['date_merge'] = start_df['date_t'].dt.date
    robot_df['date_merge'] = robot_df['date'].dt.date

    robot_df['active_robot_t'] = 1
    robot_df['lastapp_Dial_t'] = 1
    
    start_df['active_robot_t'] = start_df['active'].apply(lambda x: 1 if x == 'robot' else 0)
    start_df['lastapp_Dial_t'] = start_df['lastapp_t'].apply(lambda x: 1 if x == 'Dial' else 0)
    
    robot_df = defs.set_perevod(step_path, robot_df, type_dict, date_i, numdays)
    
    start_df['NN'] = start_df.sort_values('date_t').groupby(['date_merge', 'phone', 'hour', 'minute', 'active', 'lastapp_t'])['date'].cumcount() + 1
    robot_df['NN'] = robot_df.sort_values('date').groupby(['date_merge', 'phone', 'hour', 'minute'])['date'].cumcount() + 1
    
    del start_df['date']
    result_df = start_df.merge(robot_df, how = 'left', 
                               on = ['date_merge', 'phone', 'hour', 'minute', 'lastapp_Dial_t', 'active_robot_t', 'NN']).\
                               fillna('0')
       
    result_df['minute'] = result_df.apply(lambda row: defs.set_minute(row['minute'], row['second_x']), axis = 1)
    result_df['hour'] = result_df.apply(lambda row: defs.set_hour(row['hour'], row['minute']), axis = 1)
    result_df['minute'] = result_df['minute'].apply(defs.set_minute_60)
    
    
    result_df['NN'] = result_df.groupby(['date_merge', 'phone', 'hour', 'minute', 'active', 'lastapp_t'])['date'].cumcount() + 1
    
    result_df = result_df.merge(robot_df, how = 'left', 
                               on = ['date_merge', 'phone', 'hour', 'minute', 'lastapp_Dial_t', 'active_robot_t', 'NN']).\
                               fillna('0')
     
    col_list = ['date', 'last_step', 'client_status', 'billsec_r', 'queue_r', 'type_step', 'userid']
    
    result_df = defs.column_choose(col_list, result_df)


        
    result_df['request_r'] = result_df.apply(lambda row: defs.set_request(row['client_status'], 
                                                                     row['type_step'], 
                                                                     row['userid']), axis = 1)
    count_request = result_df[result_df['request_r'] == 1].shape[0]
    logging.info(f'count_request: {count_request}')
    
    result_df = result_df[['date_t', 'phone', 'billsec_t', 'lastapp_t', 'queue_i', 'userid',
                       'exit_point', 'exit_name', 'project', 'count', 'daily_count', 'spam', 'date', 'active',
                       'lastapp_Dial_t', 'active_robot', 'active_0', 'active_operator', 'lastapp_Dial',
                       'lastapp_Transfer', 'lastapp_Playback', 'lastapp_BackGround',
                       'lastapp_Hangup', 'lastapp_WaitExten', 'lastapp_Answer', 'lastapp_Goto', 'request_r',                       
                       'last_step', 'client_status', 'billsec_r', 'queue_r', 'type_step']]
    
    result_df.to_csv(result_path, index = False)

    logging.info(f'result_df size: {result_df.shape[0]}') 














def union_call_df(start_path, call_path, request_path, result_path, type_dict, date_i):

    logging.info(f'date_i: {date_i}')
    
    call_df = pd.read_csv(call_path, dtype = type_dict)
    logging.info(f'call_df size: {call_df.shape[0]}')
    
    start_df = pd.read_csv(start_path, dtype = type_dict)

    logging.info(f'start size: {start_df.shape[0]}')

    defs.create_times_columns(call_df, 'date')
    defs.create_times_columns(start_df, 'date_t')

    
    call_df['lastapp_Dial_t'] = 1
    call_df['date_merge'] = call_df['date'].dt.date
    start_df['date_merge'] = start_df['date_t'].dt.date
    
    
    
    del call_df['second']
    

    call_df['NN'] = call_df.groupby(['date_merge', 'phone', 'hour', 'minute'])['date'].cumcount() + 1
    start_df['NN'] = start_df.groupby(['date_merge', 'phone', 'hour', 'minute'])['date_t'].cumcount() + 1
    start_df.rename(columns = {'date' : 'date_r'}, inplace = True)
    
    result_df = start_df.merge(call_df, how = 'left', 
                               on = ['date_merge', 'phone', 'hour', 'minute', 'NN', 'lastapp_Dial_t']).fillna('0')
    
    col_list = ['userid']

    result_df = defs.column_choose(col_list, result_df) 

    del result_df['userid_x']
    del result_df['userid_y']
    
    result_df['minute'] = result_df['minute'] + 1
    
    result_df = result_df.merge(call_df, how = 'left', 
                               on = ['date_merge', 'phone', 'hour', 'minute', 'NN', 
                                     'lastapp_Dial_t']).fillna('0')
    
    col_list = ['date', 'otkaz_c', 'queue_c', 'userid']
  
    result_df = defs.column_choose(col_list, result_df)  
    
    result_df = result_df[['date_t', 'phone', 'billsec_t', 'lastapp_t', 'queue_i', 'exit_point',
       'exit_name', 'project', 'count', 'daily_count', 'spam', 'active',
       'lastapp_Dial_t', 'active_robot', 'active_0', 'active_operator',
       'lastapp_Dial', 'lastapp_Transfer', 'lastapp_Playback', 'date_r',
       'lastapp_BackGround', 'lastapp_Hangup', 'lastapp_WaitExten', 'date_merge',
       'lastapp_Answer', 'lastapp_Goto', 'request_r', 'last_step', 'userid',
       'client_status', 'billsec_r', 'queue_r', 'type_step', 'hour', 'minute',
       'second', 'NN', 'date', 'otkaz_c', 'queue_c']]
    
    result_df['minute'] = result_df['minute'] + 1

    result_df = result_df.merge(call_df, how = 'left', 
                            on = ['date_merge', 'phone', 'hour', 'minute', 'NN', 
                                    'lastapp_Dial_t']).fillna('0')

    col_list = ['date', 'otkaz_c', 'queue_c', 'userid']

    result_df = defs.column_choose(col_list, result_df) 
    logging.info(f'result_df size: {result_df.shape[0]}')
    
    request_df = pd.read_csv(request_path, dtype = type_dict)
    logging.info(f'request_df size: {request_df.shape[0]}')

    # request_df = request_df[request_df['dateentered'] >= str(date_i)][['userid', 'dateentered', 'phone', 'statused']]
    logging.info(f'request type: {request_df.info()}')
    logging.info(f'result_df type: {result_df.info()}')

    union_df = result_df.merge(request_df[['userid', 'dateentered', 'phone']], how = 'left', on = ['userid', 'phone']).fillna('0')
    logging.info(f'merge result_df&request size: {union_df.shape[0]}')

    union_df['request_c'] = 0
    union_df.loc[(union_df['dateentered'] == '0') , 'dateentered'] = pd.to_datetime('2020-01-01', format='%Y-%m-%d %H:%M:%S')
    union_df['dateentered'] =  pd.to_datetime(union_df['dateentered'], format='%Y-%m-%d %H:%M:%S')
    union_df.loc[(union_df['dateentered'] >= union_df['date_t']) , 'request_c'] = 1
    print(union_df[union_df['dateentered'] != '2020-01-01'].shape[0])

    union_df = union_df[['date_t', 'phone', 'billsec_t', 'lastapp_t', 'queue_i', 'exit_point',
       'exit_name', 'project', 'count', 'daily_count', 'spam', 'active',
       'lastapp_Dial_t', 'active_robot', 'active_0', 'active_operator',
       'lastapp_Dial', 'lastapp_Transfer', 'lastapp_Playback', 'date_r',
       'lastapp_BackGround', 'lastapp_Hangup', 'lastapp_WaitExten',
       'lastapp_Answer', 'lastapp_Goto', 'request_r', 'last_step', 'date',
       'client_status', 'billsec_r', 'queue_r', 'type_step', 'result_call', 
       'otkaz_c', 'queue_c', 'userid', 'dateentered', 'request_c']].rename(columns = {'date' : 'date_c'})

    
    union_df.to_csv(result_path, index = False)

    logging.info(f'result_df size: {union_df.shape[0]}')














def union_exit_dict(start_path, result_path, type_dict):

    table_name = 'Входящие для Отчета'

    gaz_df = google_sheet.download_gs(table_name, 'Газификация').\
                                rename(columns = {'Exit_point' : 'exit_point', 'Действие' : 'exit_name_1', 'Результат' : 'result_1'})
    gaz_df['project'] = 'gaz'
    gaz_df['exit_point'] = gaz_df['exit_point'].astype('str')
    logging.info(f'exit_point {gaz_df.exit_point.unique()}')

    telecom_df = google_sheet.download_gs(table_name, 'Телеком').\
                                rename(columns = {'Exit_point' : 'exit_point', 'Действие' : 'exit_name_2', 'Результат' : 'result_2'})
    telecom_df['project'] = '0'
    telecom_df['exit_point'] = telecom_df['exit_point'].astype('str')
    logging.info(f'exit_point {telecom_df.exit_point.unique()}')

    rtk_df = google_sheet.download_gs(table_name, 'Ростелеком').\
                                rename(columns = {'Exit_point' : 'exit_point', 'Действие' : 'exit_name_4', 'Результат' : 'result_4'})
    rtk_df['project'] = 'rtk'
    rtk_df['exit_point'] = rtk_df['exit_point'].astype('str')
    logging.info(f'exit_point {rtk_df.exit_point.unique()}')


    tarifnik_df = google_sheet.download_gs(table_name, 'Тарифник')[['did', 'action', 'result']].\
                                rename(columns = {'action' : 'exit_name_3', 'result' : 'result_3'})
    
    internet_df = google_sheet.download_gs(table_name, '101 Интернет')[['did', 'action', 'result']].\
                    rename(columns = {'action' : 'exit_name_3', 'result' : 'result_3'})

    tarifnik_df['project'] = 'tarifnik'
    tarifnik_df['did'] = tarifnik_df['did'].astype('str')
    

    
    internet_df['project'] = '101_internet'

    tarifnik_df = pd.concat([internet_df, tarifnik_df], ignore_index = True)
    tarifnik_df['exit_name_3'] = tarifnik_df['exit_name_3'].astype('str')
    
    

    start_df = pd.read_csv(start_path, dtype = type_dict)
    logging.info(f'exit_point {start_df.exit_point.unique()}')
    
    start_df['did'] = start_df['did'].astype('str')
    start_df['exit_point'] = start_df['exit_point'].astype('str')

    start_df = start_df.merge(gaz_df, how = 'left', on = ['project', 'exit_point'])
    start_df = start_df.merge(telecom_df, how = 'left', on = ['project', 'exit_point'])
    start_df = start_df.merge(rtk_df, how = 'left', on = ['project', 'exit_point'])   
    start_df = start_df.merge(tarifnik_df, how = 'left', on = ['project', 'did'])

    start_df = start_df.fillna('')
    start_df['exit_name'] = start_df['exit_name_1'] + start_df['exit_name_2'] + start_df['exit_name_3']  + start_df['exit_name_4']
    start_df['active'] = start_df['result_1'] + start_df['result_2'] + start_df['result_3'] + start_df['result_4']

    start_df.loc[start_df['active'] == 'оператор', 'active'] = 'operator'
    start_df.loc[start_df['active'] == 'сброс', 'active'] = 'hangup'
    start_df.loc[start_df['active'] == 'Спам', 'active'] = 'spam'
    start_df.loc[start_df['active'] == 'робот', 'active'] = 'robot'
    start_df.loc[start_df['exit_point'].isin(["100","102","103","105","106","107","108","109","110","113"]), 'exit_point'] = "999"
    start_df.loc[start_df['exit_point'].isin(["100","102","103","105","106","107","108","109","110","113"]), 'exit_name'] = "Спам"


    print('unique exit name 101 internet ', start_df[start_df['project'] == '101_internet']['exit_name'].unique())
    print('unique exit name tarifnik ', start_df[start_df['project'] == 'tarifnik']['exit_name'].unique())

    start_df = start_df[['date_t', 'uniqueid', 'phone', 'billsec_t', 'lastapp_t', 'hour', 'date',
           'queue_i', 'exit_point', 'project', 'daily_count', 'count', 'spam', 'exit_name', 'active']]


    start_df.to_csv(result_path, index = False)














def union_with_astin(truba_path, astin_path, type_dict, result_path):
    
    astin_df = pd.read_csv(astin_path, dtype = type_dict)
    logging.info(f'astin size: {astin_df.shape[0]}')
    print('astin unique action:', astin_df['lastapp_a'].unique())

    
    astin_df = defs.astin_grouped(astin_df)
    
    
    truba_df = pd.read_csv(truba_path, dtype = type_dict)
    logging.info(f'truba size: {truba_df.shape[0]}')
                 
    defs.truba_create_columns(truba_df) 
    
    union_df = defs.merge_dfs(truba_df, astin_df).fillna('0')
    
    count_operator = union_df[union_df['active_operator'] == 1]
    
    logging.info(f'call count: {count_operator.shape[0]}')
    
    logging.info(f'union_df size: {union_df.shape[0]}')

    print('Transfer count ', union_df[union_df['lastapp_Transfer'] == 1]['lastapp_Transfer'].sum())
    print('Dial count ', union_df[union_df['lastapp_Dial'] == 1]['lastapp_Dial'].sum())
    print('Playback count ', union_df[union_df['lastapp_Playback'] == 1]['lastapp_Playback'].sum())
    
                                                       
    union_df.to_csv(f'{result_path}', index = False)   




















  

        