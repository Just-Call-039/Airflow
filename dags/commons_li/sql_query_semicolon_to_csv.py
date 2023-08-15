# Выгрузка датасета sql скриптом из базы
def sql_query_to_csv_sc(cloud, path_sql_file, path_csv_file, name_csv_file, current_separator=';'):
    import pymysql
    import pandas as pd

    from commons.connect_db import connect_db

    host, user, password = connect_db(cloud)
    my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                 db="suitecrm",
                                 charset='utf8')

    my_query = open(path_sql_file, "r", encoding='utf8', errors='ignore').read().replace('п»ї','').replace('﻿','').replace('\ufeff','')

    df = pd.read_sql_query(my_query, my_connect)

    to_file = rf'{path_csv_file}/{name_csv_file}'
    df.to_csv(to_file, index=False, sep=current_separator, encoding='utf-8')

    my_connect.close()


def repeat_download_sc(n, days, source, cloud, path_sql_file, path_csv_file, name_csv_file):
    import pymysql
    import pandas as pd
    import datetime
    from time import sleep


    if cloud == 'Truby':

        from commons_sawa.connect_db import connect_db

        host, user, password = connect_db(cloud)
        host = host.strip('"').strip('[]').split(', ')
        step = 1


        for _ in range(0,days):
            for i in host:
                if step in source:
                    print(f'Ушел на {step} трубу, {n} день ')
                    my_connect = pymysql.Connect(host=i, user=user, passwd=password,
                                                db="asteriskcdrdb",
                                                charset='utf8')

                    my_query = open(path_sql_file).read().replace('п»ї','').replace('﻿','').replace('\ufeff','').format(n)
                    # print(my_query)

                    df = pd.read_sql_query(my_query, my_connect)
                    now = datetime.datetime.now() - datetime.timedelta(days=n)

                    name_csv_file_new = name_csv_file.format(str(step)+'_'+now.strftime("%m_%d"))
                    to_file = rf'{path_csv_file}/{name_csv_file_new}'
                    df.to_csv(to_file, index=False, sep=';', encoding='utf-8')
                    print(f'DONE {now}')

                    my_connect.close()
                    step += 1
                    sleep(20)
                else:
                    step += 1
            n += 1
            step = 1

    else:    
        from commons.connect_db import connect_db

        for i in range(0,days):
            host, user, password = connect_db(cloud)
            my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                        db="suitecrm",
                                        charset='utf8')

            my_query = open(path_sql_file).read().replace('п»ї','').replace('﻿','').replace('\ufeff','').format(n)
            # print(my_query)

            df = pd.read_sql_query(my_query, my_connect)
            now = datetime.datetime.now() - datetime.timedelta(days=n)

            name_csv_file_new = name_csv_file.format(now.strftime("%m_%d"))
            to_file = rf'{path_csv_file}/{name_csv_file_new}'
            df.to_csv(to_file, index=False, sep=';', encoding='utf-8')
            print(f'DONE {now}')

            n += 1
            my_connect.close()
            sleep(20)

# Так же функция отправки sql скрипта на сервер, но с циклом по лимиту строк, где x - начальная строка, y - количество строк, count_repeats - количество повторов
def repeat_download_data_sc(x, y, count_repeats, limit_list, cloud, path_sql_file, path_csv_file, name_csv_file, timeout):
    import pymysql
    import pandas as pd
    import datetime
    from datetime import datetime
    from time import sleep

    from commons.connect_db import connect_db

    data = pd.DataFrame()
    
    for _ in range(0,count_repeats):
        host, user, password = connect_db(cloud)
        my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                     db="suitecrm",
                                     charset='utf8')

        my_query = open(path_sql_file).read().replace('п»ї','').replace('﻿','').replace('\ufeff','').format(x,y)
        # print(my_query)
        print('Отправляем запрос')

        df = pd.read_sql_query(my_query, my_connect)

        print('Добавляем кусок к основному df')
        data = data.append(df)
        x = x + y

        print(f'Выгрузили строк = {df.shape[0]}')
        print(f'В соновном датасете строк = {data.shape[0]}')

        if x in limit_list:
            print(f'Идет сохранение файла Data_{x}')
            way = rf'{path_csv_file}/{name_csv_file}'
            data.to_csv(way.format(x), index=False, sep=',', encoding='utf-8')
            del data
            data = pd.DataFrame()
            print('Готово')
            print(datetime.today().strftime("%m/%d/%Y, %H:%M:%S"))
            # my_connect.close()
            sleep(timeout)
        elif x > limit_list[-1]:
            print(f'Идет сохранение файла Data_{x}')
            way = rf'{path_csv_file}/{name_csv_file}'
            data.to_csv(way.format(x), index=False, sep=',', encoding='utf-8')
            del data
            data = pd.DataFrame()
            print('Готово')
            print(datetime.today().strftime("%m/%d/%Y, %H:%M:%S"))
            # my_connect.close()
            sleep(timeout)

        print(f'Закрываем коннект на {timeout} секунд')
        my_connect.close()
        sleep(timeout)

def sql_query_to_csv2_sc(cloud, db, path_sql_file, path_csv_file, name_csv_file, current_separator=','):
    import pymysql
    import pandas as pd

    from commons.connect_db import connect_db

    host, user, password = connect_db(cloud)
    my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                 db=db,
                                 charset='utf8')

    my_query = open(path_sql_file, "r", encoding='utf8', errors='ignore').read().replace('п»ї','').replace('﻿','').replace('\ufeff','')

    df = pd.read_sql_query(my_query, my_connect)

    to_file = rf'{path_csv_file}/{name_csv_file}'
    df.to_csv(to_file, index=False, sep=current_separator, encoding='utf-8')

    my_connect.close()

