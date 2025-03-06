def calls_to_clickhouse(path_to_sql_calls, csv_calls):
    import pandas as pd
    import glob
    import os
    from clickhouse_driver import Client
    from commons_liza import to_click
    from time import sleep

    full_calls = pd.DataFrame()
    all_files = len(os.listdir(path_to_sql_calls))
    print(f'Всего файлов {all_files}')
    n = 0
    stop = 7
    for i in range(0,stop):
        print(f'Отправляем только {stop}')
        files = sorted(glob.glob(path_to_sql_calls + "/*.csv"),reverse=True)

        print(f'Текущий файл # {n+1}')
        print(files[n])

        print('Читаем файл')
        calls = pd.read_csv(files[n], sep=',')

        full_calls = full_calls.append(calls)
        n += 1

    full_calls = full_calls[full_calls['queue'] != '50-n']
    print('Редактируем формат')
    full_calls['call_date'] = pd.to_datetime(full_calls['call_date'])
    full_calls[['etv','calls','was_ptv','perevod','perevelys','billsec','call_hour','call_minute',
        'count_steps','last_step','destination_queue','queue']] = full_calls[['etv','calls','was_ptv','perevod','perevelys','billsec','call_hour','call_minute',
                                                                            'count_steps','last_step','destination_queue','queue']].fillna(0).astype('int64')
    full_calls[['directory','assigned_user_id','client_status','otkaz','inbound_call','was_stepgroups','type_ro',
        'network_provider_c','marker','region_name','town_name','city_name']] = full_calls[['directory','assigned_user_id','client_status','otkaz','inbound_call','was_stepgroups','type_ro',
                                                            'network_provider_c','marker','region_name','town_name','city_name']].fillna('').astype('str')
    
    cluster = '{cluster}'
    sql_request = f'''truncate table report_25_last_week on cluster '{cluster}' '''

    to_click.delete_data(sql_request) 

    sleep(600)

    to_click.save_df('report_25_last_week', full_calls)
   