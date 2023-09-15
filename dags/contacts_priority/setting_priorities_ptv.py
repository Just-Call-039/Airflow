def setting_priorities_ptv(path_sql_file):
    import pandas as pd
    from clickhouse_driver import Client

    def priority1(row):
        X = row['list2'].split(',')
        Y = row['list'].split(',')
        pr = [x for _,x in sorted(zip(Y,X))][0:1]
        pr = str(pr).replace('[','').replace(']','').replace("'",'')
        return pr

    def priority2(row):
        X = row['list2'].split(',')
        Y = row['list'].split(',')
        pr2 = [x for _,x in sorted(zip(Y,X))][1:2]
        pr2 = str(pr2).replace('[','').replace(']','').replace("'",'')
        return pr2
    
    print('Подключаемся к clickhouse')
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

    n = 0

    for _ in range (0,2):

        n += 1

        if n == 1:
            print('ЧАСТЬ ПЕРВАЯ ___________________________________________')
            print('Выгружаем нашу разметку')
        elif n == 2:
            print('ЧАСТЬ ВТОРАЯ ___________________________________________')
            print('Выгружаем не нашу разметку')
        elif n == 3:
            print('ЧАСТЬ ТРЕТЬЯ ___________________________________________')
            print('Выгружаем холод')

        client = Client(host=host, port='9000', user=user, password=password,
                        database='suitecrm_robot_ch', settings={'use_numpy': True})

        sql = open(path_sql_file).read().format(n)
        df = pd.DataFrame(client.query_dataframe(sql))


        print('Проставляем приоритеты')
        print(1)
        df['priority1'] = df.apply(lambda row: priority1(row), axis=1)
        print(2)
        df['priority2'] = df.apply(lambda row: priority2(row), axis=1)

        bln = df[(df['ptv'] == n) & ((df['priority1'] == 'bln') | (df['priority2'] == 'bln'))].id_custom.count()
        mts = df[(df['ptv'] == n) & ((df['priority1'] == 'mts') | (df['priority2'] == 'mts'))].id_custom.count()
        ttk = df[(df['ptv'] == n) & ((df['priority1'] == 'ttk') | (df['priority2'] == 'ttk'))].id_custom.count()
        nbn = df[(df['ptv'] == n) & ((df['priority1'] == 'nbn') | (df['priority2'] == 'nbn'))].id_custom.count()
        dom = df[(df['ptv'] == n) & ((df['priority1'] == 'dom') | (df['priority2'] == 'dom'))].id_custom.count()
        rtk = df[(df['ptv'] == n) & ((df['priority1'] == 'rtk') | (df['priority2'] == 'rtk'))].id_custom.count()


        if n == 1:
            print('Разметка Наша по приоритетам')
            print('')
        elif n == 2:
            print('Разметка Не наша по приоритетам')
            print('')
        elif n == 3:
            print('Холод по приоритетам')
            print('')

        print(f'Билайн {bln:,}'.replace(',', ' '))
        print(f'МТС {mts:,}'.replace(',', ' '))
        print(f'ТТК {ttk:,}'.replace(',', ' '))
        print(f'НБН {nbn:,}'.replace(',', ' '))
        print(f'ДомРу {dom:,}'.replace(',', ' '))
        print(f'РТК {rtk:,}'.replace(',', ' '))


        df2 = df[df['priority1'] != ''][['id_custom',
                                'ptv',
                                'priority1','priority2']]


        print('Заливаем в итоговую таблицу')
        client = Client(host=host, port='9000', user=user, password=password,
                        database='suitecrm_robot_ch', settings={'use_numpy': True})

        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.contacts_priorities VALUES', df2)