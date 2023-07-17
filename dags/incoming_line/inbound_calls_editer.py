def inbound_editer(path_to_files, inbound, operator_calls, request, path_result, file_result):
    import pandas as pd
    import inbound_calls.defs as defs
    import fsp.def_project_definition as def_project_definition

    team_project = def_project_definition.team_project()
    queue_project = def_project_definition.queue_project()

    projects_dict = {'BEELINE': 'BEELINE LIDS',
                    'MTS': 'MTS LIDS',
                    'яMTS': 'MTS LIDS',
                    'RTK': 'RTK LIDS',
                    'NBN': 'NBN LIDS',
                    'TTK': 'TTK LIDS',
                    'DOMRU': 'DOMRU LIDS',
                    'яBEELINE': 'BEELINE LIDS',
                    'яRTK': 'RTK LIDS',
                    'яNBN': 'NBN LIDS',
                    'яTTK': 'TTK LIDS',
                    'DR': 'DR',
                    'яDR': 'DR',
                    'яDOMRU': 'DOMRU LIDS',
                    'JC': 'JC',
                    'яJC': 'JC',
                    'JC MTS': 'MTS',
                    'яMixtell JC': 'Mixtell JC',
                    'Mixtell JC': 'Mixtell JC',
                    'Beeline': 'BEELINE LIDS',
                    'Dom Ru': 'DOMRU LIDS',
                    'яBeeline': 'BEELINE LIDS',
                    'яDom Ru': 'DOMRU LIDS'}

    team_project['date'] = pd.to_datetime(team_project['date'])
    team_project['team'] = team_project['team'].astype('str')

    queue_project['date'] = pd.to_datetime(queue_project['date'])
    queue_project['Очередь'] = queue_project['Очередь'].astype('str')

    print('Входящая линия')
    calls_in = pd.read_csv(f'{path_to_files}/{inbound}').fillna('').rename(columns={'name': 'calltype'})
    calls_in['calldate'] = pd.to_datetime(calls_in['calldate'])

    print('Звонки операторов')
    calls_out = pd.read_csv(f'{path_to_files}/{operator_calls}').fillna('')
    calls_out['calldate'] = pd.to_datetime(calls_out['calldate'])

    print('Заявки')
    request = pd.read_csv(f'{path_to_files}/{request}').fillna('')
    request['date_entered'] = pd.to_datetime(request['date_entered'])
    request['phone_work'] = request['phone_work'].astype('str')

    print('Находим предыдущий операторский вызов')
    phones = calls_in['asterisk_caller_id_c'].to_list()
    calls_out_phones = calls_out.query('asterisk_caller_id_c in @phones')
    calls_out_phones = calls_out_phones[['asterisk_caller_id_c','assigned_user_id','result_call_c','calldate','queue_c']].rename(columns={'assigned_user_id': 'operator',
                                                                                                'calldate': 'operator_calldate',
                                                                                                 'result_call_c': 'operator_resultcall'       })
    calls_out_phones['RN'] = calls_out_phones.sort_values(['operator_calldate'], ascending=[False]).groupby(['asterisk_caller_id_c']).cumcount() + 1

    calls_out_RN1 = calls_out_phones[calls_out_phones['RN'] == 1]
    calls_out_RN2 = calls_out_phones[calls_out_phones['RN'] == 2]
    calls_out_RN3 = calls_out_phones[calls_out_phones['RN'] == 3]

    calls_out_RN = calls_out_RN1.merge(calls_out_RN2, how='left', on='asterisk_caller_id_c')

    calls_out_RN = calls_out_RN.merge(calls_in[['asterisk_caller_id_c','calldate']], how='left', on='asterisk_caller_id_c')

    calls_out_RN['calldate1'] = calls_out_RN['operator_calldate_x']-calls_out_RN['operator_calldate_y']
    calls_out_RN.fillna('', inplace=True)

    calls_out_RN['operatorcalldate'] = calls_out_RN.apply(lambda row: defs.calldate(row), axis=1 )
    calls_out_RN['operator'] = calls_out_RN.apply(lambda row: defs.operator(row), axis=1 )
    calls_out_RN['operatorqueue'] = calls_out_RN.apply(lambda row: defs.operatorqueue(row), axis=1 )
    calls_out_RN['operatorresultcall'] = calls_out_RN.apply(lambda row: defs.operatorresultcall(row), axis=1 )

    calls_out_RN = calls_out_RN[['calldate','asterisk_caller_id_c','operatorcalldate','operator','operatorqueue','operatorresultcall']].drop_duplicates()
    calls_in_last = calls_in.merge(calls_out_RN, how='left', on=['asterisk_caller_id_c','calldate']).fillna('')

    print(f'Входящие {calls_in.shape[0]}')
    print(f'Операторские {calls_in_last.shape[0]}')
    print(f'Разница {calls_in.shape[0] - calls_in_last.shape[0]}')

    calls_in_last = calls_in_last[['calltype','contact_id', 'asterisk_caller_id_c', 'calldate', 'assigned_user_id', 'result_call_c', 'otkaz_c', 'ne_reshena_c','reshena_c', 'operatorcalldate', 'operator','operatorqueue','operatorresultcall']]

    calls_in_last['calldate'] = pd.to_datetime(calls_in_last['calldate'])
    calls_in_last['asterisk_caller_id_c'] = calls_in_last['asterisk_caller_id_c'].astype('str')

    print('Выгружаем роботлог')
    robotlog = defs.robotlog_phones(calls_in_last)

    print('Чистим индексы')
    robotlog = robotlog.reset_index(drop=True)
    # robotlog = robotlog[['phone','queue','call_date']].drop_duplicates()

    print('Нумеруем строки')
    robotlog['RN'] = robotlog.sort_values(['call_date'], ascending=[False]).groupby(['phone']).cumcount() + 1

    robotlog_RN1 = robotlog[robotlog['RN'] == 1]
    robotlog_RN2 = robotlog[robotlog['RN'] == 2]
    robotlog_RN3 = robotlog[robotlog['RN'] == 3]

    robotlog_RN = robotlog_RN1.merge(robotlog_RN2, how='left', on='phone').rename(columns={'queue_x': 'queue_1',
                                                                                            'call_date_x': 'call_date_1',
                                                                                            'RN_x': 'RN_1',
                                                                                            'queue_y': 'queue_2',
                                                                                            'call_date_y': 'call_date_2',
                                                                                            'RN_y': 'RN_2'})
    robotlog_RN = robotlog_RN.merge(robotlog_RN3, how='left', on='phone').rename(columns={'queue': 'queue_3',
                                                                                            'call_date': 'call_date_3',
                                                                                            'RN': 'RN_3'})

    print('Соединяем входящие с роботлогом')
    calls_in_last_rn = calls_in_last.merge(robotlog_RN, how='left', left_on=['asterisk_caller_id_c'], right_on=['phone']).fillna('')

    calls_in_last_rn['first_calldate'] = calls_in_last_rn.apply(lambda row: defs.calldate2(row), axis=1 )
    calls_in_last_rn['first_queue']    = calls_in_last_rn.apply(lambda row: defs.queue2(row), axis=1 )

    calls_in_last_rn = calls_in_last_rn[['calltype','contact_id','asterisk_caller_id_c', 'calldate', 'assigned_user_id',
       'result_call_c', 'otkaz_c', 'ne_reshena_c', 'reshena_c',
       'first_calldate', 'first_queue', 'operator','operatorresultcall']]

    users = pd.read_csv('/root/airflow/dags/fsp/Files/users.csv')
    users.fillna('', inplace=True)
    users['project'] = users['supervisor'].apply(lambda x: x.split('-')[0].strip(' ').strip('я').strip('_'))
    users['project_user'] = users.apply(lambda row: defs.user_project(row), axis=1)
    users = users.replace({"project_user": projects_dict})


    calls_in_last_rn = calls_in_last_rn.merge(users[['id','team','project_user']], how='left', left_on=['operator'], right_on=['id'])
    calls_in_last_rn = calls_in_last_rn.merge(team_project, how='left', left_on=['team','first_calldate'], right_on=['team','date'])
    calls_in_last_rn = calls_in_last_rn.merge(queue_project, how='left', left_on=['first_queue','first_calldate'], right_on=['Очередь','date'])

    calls_in_last_rn['project'] = calls_in_last_rn.apply(lambda row: defs.project(row), axis=1)
    x = calls_in_last_rn[calls_in_last_rn['operator'] == '43dd8772-489b-a5a6-d063-63f486ea6466']['project']
    print(f'Проверка проекта {x}')

    print('Разбираем заявки')
    users['project'] = users['project_user'].apply(lambda x: x.replace(' LIDS',''))

    request = request.merge(users[['id','team','project','project_user']], how='left', left_on='assigned_user_id', right_on='id').merge(team_project, how='left', left_on=['team','date_entered'], right_on=['team','date']).fillna('')
    request['team_project2'] = request['team_project'].apply(lambda x: x.replace(' LIDS',''))
    print(request.columns)
    request['final_project'] = request.apply(lambda row: defs.request_project(row), axis=1)
    request = request[['last_queue_c', 'date_entered', 'status', 'konva', 'rtkid',
       'phone_work', 'assigned_user_id', 'final_project']]
    
    print('Соединяем звонки с заявками')
    calls_in_last_rnr = calls_in_last_rn.merge(request, how='left', left_on=['asterisk_caller_id_c','assigned_user_id'], right_on=['phone_work','assigned_user_id'])
    
    calls_phones = calls_in_last_rnr[['calltype','contact_id','asterisk_caller_id_c', 'calldate',
                                            # 'assigned_user_id_x',
                                            'assigned_user_id',
   'result_call_c', 'otkaz_c', 'ne_reshena_c', 'reshena_c',
   'first_calldate', 'first_queue', 'operator','operatorresultcall','project', 'final_project','last_queue_c', 'date_entered', 'status', 'konva']].rename(columns={'final_project': 'request_project',
   'last_queue_c': 'request_queue'
#    ,'assigned_user_id_x': 'assigned_user_id'
   }).fillna('')
    
    
    calls_in_last_rnr = calls_in_last_rnr[['calltype','contact_id', 'calldate',
                                            # 'assigned_user_id_x',
                                            'assigned_user_id',
   'result_call_c', 'otkaz_c', 'ne_reshena_c', 'reshena_c',
   'first_calldate', 'first_queue', 'operator','operatorresultcall','project', 'final_project','last_queue_c', 'date_entered', 'status', 'konva']].rename(columns={'final_project': 'request_project',
   'last_queue_c': 'request_queue'
#    ,'assigned_user_id_x': 'assigned_user_id'
   }).fillna('')
    
    calls_phones_file = 'calls_phones.csv'

    print('Сохраняем файл')
    to_file = rf'{path_result}/{file_result}'
    calls_in_last_rnr.to_csv(to_file, index=False, sep=',', encoding='utf-8')
    calls_phones.to_csv(rf'{path_result}/{calls_phones_file}', index=False, sep=',', encoding='utf-8')


    