


import pandas as pd
from commons_liza import to_click
from datetime import datetime
import report_25_today.defs as defs
from fsp.def_project_definition import queue_project2
from commons_liza.load_mysql import get_data_request

# Скрипт для редактирования потеярнных данных


def editor_lost(path_to_sql_transfer_steps, sql_transfer_steps, sql_steps, sql_transfers,sql_city,sql_town,sql_region, path_to_calls):
    
    date_list = ['2025-01-08', '2025-01-09', '2025-01-10', '2025-01-11', '2025-01-12']
    cloud = ['base_dep_slave', 'IyHBh9mDBdpg', '192.168.1.183', 'suitecrm']

    for date_i in date_list:

        print(date_i)

        call_sql = '''select assigned_user_id,
                        contact_id_c,
                        call_date,
                        last_step,
                        count_steps,
                        uniqueid,
                        client_status,
                        otkaz,
                        was_repeat,
                        REGEXP_SUBSTR(dialog, '[0-9]+') queue,
                        route,
                        server_number,
                        directory,
                        billsec,
                        town,
                        inbound_call,
                        marker,
                        was_stepgroups,
                        ptv_c,
                        network_provider_c,
                        city_c,
                        region_c,
                        phone
                    from suitecrm_robot.jc_robot_log
                    where date(call_date) = '{date_i}'

                    union all

                    select operator_id assigned_user_id,
                        contact_id contact_id_c,
                        call_date,
                        last_step,
                        1 as count_steps,
                        dialog_id uniqueid,
                        client_status,
                        refuse otkaz,
                        was_ptv was_repeat,
                        robot_id queue,
                        route,
                        server_number,
                        voice directory,
                        billsec,
                        region as town,
                        direction inbound_call,
                        marker,
                        0 was_stepgroups,
                        ptv ptv_c,
                        network_provider network_provider_c,
                        city city_c,
                        quality region_c,
                        phone
                    from suitecrm_robot.robot_log 
                        left join suitecrm_robot.robot_log_addition 
                        on robot_log.id = robot_log_addition.robot_log_id
                    where date(call_date) = '{date_i}' '''

        calls = get_data_request(call_sql.format(date_i=date_i), cloud)
        print('calls size ', calls.shape[0])

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
        
        print('-- переводы')
        calls['perevod'] = calls.apply(lambda row: defs.perevod(row), axis=1)
        calls['perevelys'] = calls.apply(lambda row: defs.perevelys(row), axis=1)
        print('-- качества')
        calls['network_provider_c'] = calls['network_provider_c'].astype('str').apply(lambda x: defs.network_provider_c(x))
        calls['region'] = calls.apply(lambda row: defs.region(row), axis=1)
        

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

        print('Сохраняем файл')
        date = datetime.strptime(date_i, "%Y-%m-%d").date()
        
        month = date.month
        day = date.day
        file_name = f'calls_last_week_{month:02}_{day:02}.csv'
        to_file = rf'{path_to_calls}/{file_name}'
        calls.to_csv(to_file, index=False, sep=',', encoding='utf-8')
        print('downloaded', to_file)



def calls_lost_archive_ch(path_to_sql_calls):
    
    date_list = ['01_08', '01_09', '01_10', '01_11', '01_12']
   
    
    
    
    for date_i in date_list:
        file = f'{path_to_sql_calls}calls_last_week_{date_i}.csv'
        print(file)

        print('Читаем файл')
        calls = pd.read_csv(file)
        calls = calls[calls['queue'] != '50-n']
        print('Редактируем формат')
        calls['call_date'] = pd.to_datetime(calls['call_date'])
        calls[['etv','calls','was_ptv','perevod','perevelys','billsec','call_hour','call_minute',
            'count_steps','last_step','destination_queue','queue']] = calls[['etv','calls','was_ptv','perevod','perevelys','billsec','call_hour','call_minute',
                                                                                'count_steps','last_step','destination_queue','queue']].fillna(0).astype('int64')
        calls[['directory','assigned_user_id','client_status','otkaz','inbound_call','was_stepgroups','type_ro',
            'network_provider_c','marker','region_name','town_name','city_name']] = calls[['directory','assigned_user_id','client_status','otkaz','inbound_call','was_stepgroups','type_ro',
                                                                'network_provider_c','marker','region_name','town_name','city_name']].fillna('').astype('str')
        
        print('Подключаемся к серверу')
        try:
            client = to_click.my_connection()

            print('Отправляем запрос')
            client.insert_dataframe('INSERT INTO suitecrm_robot_ch.report_25_archive VALUES', calls)  
        except (ValueError):
            print('Данные не загружены')
        finally:

            client.connection.disconnect()
            print('conection closed')
              
        del calls
