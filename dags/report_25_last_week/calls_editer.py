def robotlog_calls_transformation(path_to_sql_calls, sql_calls, path_to_sql_transfer_steps, sql_transfer_steps, sql_steps, sql_transfers,sql_city,sql_town,sql_region, path_to_calls):
    import pandas as pd
    import glob
    import os
    import report_25_today.defs as defs
    from fsp.def_project_definition import queue_project2

    print('Звонки')

    # full_calls = pd.DataFrame()
    all_files = len(os.listdir(path_to_sql_calls))
    print(f'Всего файлов {all_files}')
    n = 0
    stop = 7
    for i in range(0,stop):
        files = sorted(glob.glob(path_to_sql_calls + "/*.csv"),reverse=True)
        
        print(f'Текущий файл # {n+1}')
        print(files[n])
        file_name = files[n].replace(path_to_sql_calls,'').strip('\\n')
        print(file_name)

        calls = pd.read_csv(files[n], sep=',')

        print(calls.head(2))
        calls = calls.fillna('')
        calls['last_step'] = calls['last_step'].astype('str').apply(lambda x: x.replace('.0',''))
        calls['queue'] = calls['queue'].astype('str').apply(lambda x: x.replace('.0',''))
        calls['phone'] = calls['phone'].astype('str').apply(lambda x: x.replace('.0',''))
        calls['call_date'] = pd.to_datetime(calls['call_date'])
        print('-- дата')
        calls['call_hour'] = pd.to_datetime(calls['call_date']).apply(lambda x: x.hour + 3)
        calls['call_minute'] = pd.to_datetime(calls['call_date']).apply(lambda x: x.minute)
        calls['call_date'] = pd.to_datetime(calls['call_date']).apply(lambda x: x.date())

        print('Шаги переводов')
        transfer_steps = pd.read_csv(f'{path_to_sql_transfer_steps}/{sql_transfer_steps}').fillna('')
        transfer_steps['step'] = transfer_steps['step'].astype('str').apply(lambda x: x.replace('.0',''))
        transfer_steps['ochered'] = transfer_steps['ochered'].astype('str').apply(lambda x: x.replace('.0',''))

        print('Переводы')
        transfers = pd.read_csv(f'{path_to_sql_transfer_steps}/{sql_transfers}').fillna('')
        transfers['phone'] = transfers['phone'].astype('str').apply(lambda x: x.replace('.0',''))
        transfers['dialog'] = transfers['dialog'].astype('str').apply(lambda x: x.replace('.0',''))
        transfers['date'] = pd.to_datetime(transfers['date']).apply(lambda x: x.date())
        print(transfers.shape)

        print('Описание шагов')
        steps = pd.read_csv(f'{path_to_sql_transfer_steps}/{sql_steps}').fillna('')
        steps['queue'] = steps['queue'].astype('str')

        print('Справочники')
        city = pd.read_csv(f'{path_to_sql_transfer_steps}/{sql_city}').fillna('')
        city['city_c'] = city['city_c'].astype('str')
        town = pd.read_csv(f'{path_to_sql_transfer_steps}/{sql_town}').fillna('')
        town['town'] = town['town'].astype('str')
        region = pd.read_csv(f'{path_to_sql_transfer_steps}/{sql_region}').fillna('')
        region['region'] = region['region'].astype('str')

        queue_project = queue_project2().fillna(0)
        queue_project['date'] = pd.to_datetime(queue_project['date']).apply(lambda x: x.date())
        queue_project = queue_project.rename(columns={'Очередь': 'queue'})
        queue_project['queue'] = queue_project['queue'].astype('int').astype('str')
        print(queue_project.head(3))

        print('Соединяем')
        calls = calls.merge(transfer_steps, left_on = ['last_step','queue'], right_on = ['step','ochered'], how = 'left')
        calls = calls.merge(steps, how='left',on='queue')
        calls = calls.merge(transfers,how='left', left_on = ['phone','queue','call_date'], right_on = ['phone','dialog','date'])
        calls.fillna('', inplace=True)

        print('Редактируем')
        # print('-- дата')
        # calls['call_hour'] = pd.to_datetime(calls['call_date']).apply(lambda x: x.hour + 3)
        # calls['call_minute'] = pd.to_datetime(calls['call_date']).apply(lambda x: x.minute)
        # calls['call_date'] = pd.to_datetime(calls['call_date']).apply(lambda x: x.date())
        print('-- переводы')
        calls['perevod'] = calls.apply(lambda row: defs.perevod(row), axis=1)
        calls['perevelys'] = calls.apply(lambda row: defs.perevelys(row), axis=1)
        print('-- качества')
        calls['network_provider_c'] = calls['network_provider_c'].astype('str').apply(lambda x: defs.network_provider_c(x))
        calls['region'] = calls.apply(lambda row: defs.region(row), axis=1)
        # calls['region'] = ''

        print('Крепим Тип РО')
        calls = calls.merge(queue_project, how='left',left_on=['queue','call_date'],right_on=['queue','date'])

        print('Определяем шаги')
        calls['hello_end'] = calls['hello_end'].astype('str').apply(lambda x: x.replace('.0',''))
        calls['steps_inconvenient'] = calls['steps_inconvenient'].astype('str').apply(lambda x: x.replace('.0',''))
        calls['steps_error'] = calls['steps_error'].astype('str').apply(lambda x: x.replace('.0',''))
        calls['steps_refusing'] = calls['steps_refusing'].astype('str').apply(lambda x: x.replace('.0',''))
        calls['top_recall'] = calls['top_recall'].astype('str').apply(lambda x: x.replace('.0',''))
        calls['welcome_end'] = calls['welcome_end'].astype('str').apply(lambda x: x.replace('.0',''))
        calls['ntv'] = calls['ntv'].astype('str').apply(lambda x: x.replace('.0',''))
        calls['abonent'] = calls['abonent'].astype('str').apply(lambda x: x.replace('.0',''))
        calls['last_step'] = calls['last_step'].astype('str').apply(lambda x: x.replace('.0',''))


        calls['description'] = calls.apply(lambda row: defs.last_step(row), axis=1)
        calls['etv'] = calls.apply(lambda row: defs.etv(row), axis=1)

        print('Группируем')
        calls = calls.groupby(['call_date',
        'call_hour',
        'call_minute',
        'queue',
        'destination_queue',
        'directory',
        'assigned_user_id',
        'last_step',
        'description',
        'count_steps',
        'client_status',
        'otkaz',
        'inbound_call',
        'region',
        'marker',  
        'network_provider_c',
        'city_c',
        'town',
        'etv',
        'type_ro',
        'was_stepgroups'],as_index=False, dropna=False).agg({'contact_id_c': 'count',
                                                    'was_repeat': 'sum',
                                                    'perevod': 'sum',
                                                    'perevelys': 'sum',
                                                    'billsec': 'sum'}).rename(columns={'contact_id_c': 'calls',
                                                                                        'was_repeat': 'was_ptv'})
        
        print('Заменяем названия справочниками')
        calls[['city_c','town','region']] = calls[['city_c','town','region']].astype('str')
        calls = calls.merge(city, how='left', on='city_c')
        calls = calls.merge(town, how='left', on='town')
        calls = calls.merge(region, how='left', on='region')

        calls = calls[['call_date',
        'call_hour',
        'call_minute',
        'queue',
        'destination_queue',
        'directory',
        'assigned_user_id',
        'last_step',
        'description',
        'count_steps',
        'client_status',
        'otkaz',
        'inbound_call',
        'region',
        'marker',  
        'network_provider_c',
        'city_c',
        'town',
        'etv',
        'was_stepgroups',
        'city_name',
        'town_name',
        'region_name',
        'type_ro',
        'calls',
        'was_ptv',
        'perevod',
        'perevelys',
        'billsec']]



        # full_calls = full_calls.append(calls)
        # full_calls = pd.concat([full_calls,calls], axis=1)

        print('Сохраняем файл')
        to_file = rf'{path_to_calls}/{file_name}'
        calls.to_csv(to_file, index=False, sep=',', encoding='utf-8')
        n += 1

    
    
    # print('Сохраняем файл')
    # full_calls.to_csv(f'{path_to_calls}/{sql_calls}', sep=',', index=False, encoding='utf-8')