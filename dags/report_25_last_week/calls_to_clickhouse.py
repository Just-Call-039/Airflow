def calls_to_clickhouse(path_to_sql_calls, csv_calls):
    import pandas as pd
    import glob
    import os
    from clickhouse_driver import Client

    full_calls = pd.DataFrame()
    all_files = len(os.listdir(path_to_sql_calls))
    print(f'Всего файлов {all_files}')
    n = 0
    for i in range(0,all_files):
        print(f'Текущий файл # {n+1}')
        print(os.listdir(path_to_sql_calls)[n])

        print('Читаем файл')
        files = glob.glob(path_to_sql_calls + "/*.csv")
        calls = pd.read_csv(files[n], sep=',')

        full_calls = full_calls.append(calls)
        n += 1

       # calls = pd.read_csv(f'{path_to_sql_calls}/{calls}')
    print('Редактируем формат')
    full_calls['call_date'] = pd.to_datetime(full_calls['call_date'])
    full_calls[['etv','calls','was_ptv','perevod','perevelys','billsec','call_hour','call_minute',
        'count_steps','last_step','destination_queue','queue']] = full_calls[['etv','calls','was_ptv','perevod','perevelys','billsec','call_hour','call_minute',
                                                                            'count_steps','last_step','destination_queue','queue']].fillna(0).astype('int64')
    full_calls[['directory','assigned_user_id','client_status','otkaz','inbound_call','was_stepgroups','type_ro',
        'network_provider_c','marker','region_name','town_name','city_name']] = full_calls[['directory','assigned_user_id','client_status','otkaz','inbound_call','was_stepgroups','type_ro',
                                                            'network_provider_c','marker','region_name','town_name','city_name']].fillna('').astype('str')



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
    
    print('Удаляем таблицу')
    client.execute('drop table suitecrm_robot_ch.report_25_last_week')

    print('Создаем таблицу')
    sql_create = '''create table suitecrm_robot_ch.report_25_last_week
                    (
                        call_date          Date,
                        call_hour          Int64,
                        call_minute        Int64,
                        queue              Int64,
                        destination_queue  Int64,
                        directory          String,
                        assigned_user_id   String,
                        last_step          Int64,
                        description        String,
                        etv                Int64,
                        count_steps        Int64,
                        client_status      String,
                        otkaz              String,
                        inbound_call       String,
                        region_name        String,
                        marker             String,
                        network_provider_c String,
                        city_name          String,
                        town_name          String,
                        type_ro            String,
                        calls              Int64,
                        was_ptv            Int64,
                        perevod            Int64,
                        perevelys          Int64,
                        billsec            Int64,
                        was_stepgroups     String
                    ) ENGINE = MergeTree
                        order by call_date'''
    client.execute(sql_create)
    print(full_calls.dtypes)
    
    print('Отправляем запрос')
    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.report_25_last_week VALUES', full_calls)
