def truby_transformation(n, days, rl_path, ll_path, truby_path, full_data_path, full_data_name):
    import pandas as pd
    import os
    import glob

    print('Обработка файлов')
    print(f'Всего файлов robot_log {len(os.listdir(rl_path))}')
    print(f'Всего файлов leg_log {len(os.listdir(ll_path))}')

    n -= 1
    for i in range(0,days):
        print(f'Текущий цикл # {n+1}')

        print(os.listdir(rl_path)[n])
        rl_files = glob.glob(rl_path + "/*.csv")
        r_log = pd.read_csv(rl_files[n])

        print(os.listdir(ll_path)[n])
        ll_files = glob.glob(ll_path + "/*.csv")
        l_log = pd.read_csv(ll_files[n])
        # l_log = l_log.number.astype('str').apply(lambda x: x.strip('.0'))

        print('Считываем выгрузки с труб')

        step = 0
        truby = pd.DataFrame()
        truby_files = glob.glob(truby_path + "/*.csv")
        for file in truby_files:
            step += 1
            print(f'file {step}')
            df = pd.read_csv(file)
            truby = truby.append(df)

        truby.clid = truby.clid.fillna(0).astype('int64')
        truby.call_date = pd.to_datetime(truby.call_date)

        print('Объединяем robot_log и leg_log')
        print(r_log.columns)
        print(l_log)
        logs_join = r_log.merge(l_log, how = 'left', left_on = ['phone','calldate','hour'], right_on = ['number','calldate','hour'])

        logs_join.phone = logs_join.phone.fillna(0).astype('int64')
        logs_join.calldate = pd.to_datetime(logs_join.calldate)

        full_data = pd.merge(logs_join, truby, how="left", left_on = ['calldate','phone','hour'],
                            right_on = ['call_date','clid','hour'])

        print('Группировка')

        full_data = full_data.groupby(['calldate', 'group_trafic2', 'last_step', 'trunk_id',
                        'network_provider', 'city_c', 'server_number','queue','otkaz_23',
                                'billsec','real_billsec',
                        'pattern', 'gw_number', 'disposition', 'way', 'gate'],
                        as_index=False, dropna=False).agg({'phone': 'count', 'trafic': 'sum'}).rename(columns={'phone': 'calls'})

        print('Скачиваем')

        full_data_name_new = full_data_name.format(os.listdir(rl_path)[n].strip('robot_log_'))
        to_file = rf'{full_data_path}/{full_data_name_new}'
        full_data.to_csv(to_file, index=False)

        n -= 1