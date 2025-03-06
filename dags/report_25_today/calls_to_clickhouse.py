def calls_to_clickhouse(path_to_sql_calls, calls):
    import pandas as pd
    from clickhouse_driver import Client
    from commons_liza import to_click

    print('Читаем файл')
    calls = pd.read_csv(f'{path_to_sql_calls}/{calls}')

    print('Редактируем формат')
    calls['call_date'] = pd.to_datetime(calls['call_date'])
    calls[['etv','calls','was_ptv','perevod','perevelys','billsec','call_hour','call_minute',
           'count_steps','last_step','destination_queue','queue']] = calls[['etv','calls','was_ptv','perevod','perevelys','billsec','call_hour','call_minute',
                                                                            'count_steps','last_step','destination_queue','queue']].fillna(0).astype('int64')
    calls[['directory','assigned_user_id','client_status','otkaz','inbound_call','was_stepgroups','type_ro',
           'network_provider_c','marker']] = calls[['directory','assigned_user_id','client_status','otkaz','inbound_call','was_stepgroups','type_ro',
                                                             'network_provider_c','marker']].fillna('').astype('str')
    
    # print('Подключаемся к серверу')
    # dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
    # if dest:
    #     with open(dest) as file:
    #         for now in file:
    #             now = now.strip().split('=')
    #             first, second = now[0].strip(), now[1].strip()
    #             if first == 'host':
    #                 host = second
    #             elif first == 'user':
    #                 user = second
    #             elif first == 'password':
    #                 password = second
    #     # return host, user, password

    try:
        # client = Client(host=host, port='9000', user=user, password=password,
        #                 database='suitecrm_robot_ch', settings={'use_numpy': True})
        client = to_click.my_connection()
        print('Удаляем таблицу')
        cluster = '{cluster}'
        client.execute(f'''truncate table report_25_today ON CLUSTER '{cluster}' ''')
   
    except (ValueError):
            print('Данные не удалены')
    finally:
        try:

            print('Создаем таблицу')
            sql_create = '''create table if not exists report_25_today ON CLUSTER '{cluster}'
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
            
            client = to_click.my_connection()
            client.execute(sql_create)
            print(calls.dtypes)
        except (ValueError):
            print('Таблица не создана')
        finally:
            # try:
    
            print('Отправляем запрос')
            print('calls size ', calls.shape[0])
            to_click.save_df('report_25_today', calls)
                # client = to_click.my_connection()
                # client.insert_dataframe('INSERT INTO suitecrm_robot_ch.report_25_today VALUES', calls)
                # print('data downloaded')
            # except (ValueError):
            #     print('Данные не загружены')
            # finally:
    
            #     client.connection.disconnect()
            #     print('conection closed')