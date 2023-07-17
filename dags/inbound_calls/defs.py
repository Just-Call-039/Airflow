import pandas as pd
import glob
import os

def calldate(row):
    if row['calldate'] >= row['operator_calldate_x'] and row['calldate'] >= row['operator_calldate_y']:
        if row['operator_calldate_x'] in ['1 days','2 days','3 days','4 days','5 days','6 days','7 days'
                                         '8 days','9 days','10 days','11 days','12 days','13 days','14 days','15 days']:
            return row['operator_calldate_y']
        else:
            return row['operator_calldate_x']
    elif row['calldate'] >= row['operator_calldate_x'] and row['queue_c_y'] == '':
        return row['operator_calldate_x']
    elif row['calldate'] >= row['operator_calldate_y']:
        return row['operator_calldate_y']
    
def operator(row):
    if row['calldate'] >= row['operator_calldate_x'] and row['calldate'] >= row['operator_calldate_y']:
        if row['operator_calldate_x'] in ['1 days','2 days','3 days','4 days','5 days','6 days','7 days'
                                         '8 days','9 days','10 days','11 days','12 days','13 days','14 days','15 days']:
            return row['operator_y']
        else:
            return row['operator_x']
    elif row['calldate'] >= row['operator_calldate_x'] and row['queue_c_y'] == '':
        return row['operator_x']
    elif row['calldate'] >= row['operator_calldate_y']:
        return row['operator_y']
    
def operatorresultcall(row):
    if row['calldate'] >= row['operator_calldate_x'] and row['calldate'] >= row['operator_calldate_y']:
        if row['operator_calldate_x'] in ['1 days','2 days','3 days','4 days','5 days','6 days','7 days'
                                         '8 days','9 days','10 days','11 days','12 days','13 days','14 days','15 days']:
            return row['operator_resultcall_y']
        else:
            return row['operator_resultcall_x']
    elif row['calldate'] >= row['operator_calldate_x'] and row['queue_c_y'] == '':
        return row['operator_resultcall_x']
    elif row['calldate'] >= row['operator_calldate_y']:
        return row['operator_resultcall_y']
    
def operatorqueue(row):
    if row['calldate'] >= row['operator_calldate_x'] and row['calldate'] >= row['operator_calldate_y']:
        if row['operator_calldate_x'] in ['1 days','2 days','3 days','4 days','5 days','6 days','7 days'
                                         '8 days','9 days','10 days','11 days','12 days','13 days','14 days','15 days']:
            return row['queue_c_y']
        else:
            return row['queue_c_x']
    elif row['calldate'] >= row['operator_calldate_x'] and row['queue_c_y'] == '':
        return row['queue_c_x']
    elif row['calldate'] >= row['operator_calldate_y']:
        return row['queue_c_y']
    

# def robotlog_phones():
#     path = '/root/airflow/dags/report_25_last_week/Files/sql_robot_log'
#     files = glob.glob(path + "/*.csv")
#     robotlog = pd.DataFrame()
#     n = 0
#     num_of_files = len(os.listdir(path))
#     print(f'Всего файлов {num_of_files}')

#     for i in files:
#         n += 1
#         print(f'Файл №{n}')
#         df = pd.read_csv(i)
#         df = df[['phone','queue','call_date']]
#         robotlog = pd.concat([robotlog,df], axis = 0)
#     del df

#     print(f'Редактируем итог')
#     robotlog = robotlog.reset_index(drop=True)

#     robotlog['phone'] = robotlog['phone'].astype('str')
#     robotlog['queue'] = robotlog['queue'].astype('str')
#     robotlog['call_date'] = pd.to_datetime(robotlog['call_date']).apply(lambda x: x.date())
#     robotlog['call_date'] = pd.to_datetime(robotlog['call_date'])

#     return robotlog

def calldate2(row):
    if row['operator'] == '' and row['queue_1'] != '':
        if row['call_date_1'] <= row['calldate']:
            return row['call_date_1']
        elif row['queue_2'] != '' and row['call_date_2'] <= row['calldate']:
            return row['call_date_2']
        elif row['queue_3'] != '' and row['call_date_3'] <= row['calldate']:
            return row['call_date_3']
    else:
        return row['operatorcalldate']
    
    
def queue2(row):
    if row['operator'] == '' and row['queue_1'] != '':
        if row['call_date_1'] <= row['calldate']:
            return row['queue_1']
        elif row['queue_2'] != '' and row['call_date_2'] <= row['calldate']:
            return row['queue_2']
        elif row['queue_3'] != '' and row['call_date_3'] <= row['calldate']:
            return row['queue_3']
    else:
        return row['operatorqueue']

def project(row):
    if row['operator'] == '':
        return row['destination_project']
    elif row['team_project'] == '':
        return row['project_user']
    else:
        return row['team_project']
    
projects = ['BEELINE','MTS','яMTS','RTK','NBN','TTK','DOMRU','яBEELINE','яRTK','яNBN','яTTK','DR','яDR',
            'яDOMRU','JC','яJC','яMixtell JC','Mixtell JC','Beeline','Dom Ru','яBeeline','яDom Ru']

def user_project(row):
    if row['project'] in projects:
        return row['project']
    else:
        return ''

def request_project(row):
    if row['project'] == 'JC':
        return row['start_project']
    elif row['start_project'] == row['project']:
        return row['project_user'] 
    elif row['start_project'] == row['team_project2']:
        return row['team_project'] 
    else:
        row['start_project']

def robotlog_phones(calls_in_last):
    from clickhouse_driver import Client
    print('--- Начинаем работу с кликхаусом')
    print('Редактируем формат')
    calls_in_last_phones = calls_in_last[['asterisk_caller_id_c']]
    calls_in_last_phones['asterisk_caller_id_c'] = calls_in_last_phones['asterisk_caller_id_c'].fillna('').astype('str')

    
    print('Подключаемся к серверу')
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
        # return host, user, password

 
    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})


    print('Создаем таблицу')
    sql_create = '''create table suitecrm_robot_ch.inbound_calls_temporary
                    (
                        asterisk_caller_id_c String

                    ) ENGINE = MergeTree
                        order by asterisk_caller_id_c'''
    client.execute(sql_create)
    print(calls_in_last_phones.dtypes)
    
    print('Отправляем номера в кликхаус')
    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.inbound_calls_temporary VALUES', calls_in_last_phones)

    sql = '''select phone, toDate(call_date) as date, dialog
    from suitecrm_robot_ch.jc_robot_log
    where toDate(call_date) >= '2023-05-01'
    and phone in (select *
                    from suitecrm_robot_ch.inbound_calls_temporary)
    '''

    print('Читаем роботлог')
    robotlog = pd.DataFrame(client.query_dataframe(sql)).rename(columns={'dialog': 'queue','date': 'call_date'})

    print('Удаляем таблицу')
    client.execute('drop table suitecrm_robot_ch.inbound_calls_temporary')

    return robotlog