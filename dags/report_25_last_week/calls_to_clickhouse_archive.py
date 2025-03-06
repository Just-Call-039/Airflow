def calls_to_clickhouse_archive(path_to_sql_calls, csv_calls):
    import pandas as pd
    import glob
    import os
    from clickhouse_driver import Client
    from commons_liza import to_click

    
    # sql_create = '''create table suitecrm_robot_ch.report_25_archive
    #                 (
    #                     call_date          Date,
    #                     call_hour          Int64,
    #                     call_minute        Int64,
    #                     queue              Int64,
    #                     destination_queue  Int64,
    #                     directory          String,
    #                     assigned_user_id   String,
    #                     last_step          Int64,
    #                     description        String,
    #                     etv                Int64,
    #                     count_steps        Int64,
    #                     client_status      String,
    #                     otkaz              String,
    #                     inbound_call       String,
    #                     region_name        String,
    #                     marker             String,
    #                     network_provider_c String,
    #                     city_name          String,
    #                     town_name          String,
    #                     type_ro            String,
    #                     calls              Int64,
    #                     was_ptv            Int64,
    #                     perevod            Int64,
    #                     perevelys          Int64,
    #                     billsec            Int64,
    #                     was_stepgroups     String
    #                 ) ENGINE = MergeTree
    #                     order by call_date'''
    # client.execute(sql_create)

    all_files = len(os.listdir(path_to_sql_calls))
    print(f'Всего файлов {all_files}')
    n = 7
    
    
    for _ in range(n, all_files + 1):
        files = sorted(glob.glob(path_to_sql_calls + "/*.csv"),reverse=True)
        print(files)

        print(f'Текущий файл # {n+1}')
        print(files[n])

        print('Читаем файл')
        calls = pd.read_csv(files[n], sep=',')
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
        
        n += 1
        del calls

        if n == 8:
        
            break
