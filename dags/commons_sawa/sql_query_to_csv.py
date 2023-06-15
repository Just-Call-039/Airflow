# Функция для выполнения SQL запроса и записи в файл.
# Необходимо передать наименование облака, полный путь и имя SQL файла, путь к csv файлу без последней /, имя csv файла,
# разделитель при необходимости.


def repeat_download(n, days, source, cloud, path_sql_file, path_csv_file, name_csv_file):
    import pymysql
    import pandas as pd
    import datetime
    from time import sleep


    if cloud == 'Truby':

        from commons_sawa.connect_db import connect_db

        host, user, password = connect_db(cloud)
        step = 1


        for i in range(0,days):
            for i in host:
                if step in source:
                    print(f'Ушел на {step} трубу')
                    my_connect = pymysql.Connect(host=i, user=user, passwd=password,
                                                db="asteriskcdrdb",
                                                charset='utf8')

                    my_query = open(path_sql_file).read().replace('п»ї','').replace('﻿','').replace('\ufeff','').format(n)
                    # print(my_query)

                    df = pd.read_sql_query(my_query, my_connect)
                    now = datetime.datetime.now() - datetime.timedelta(days=n)

                    name_csv_file_new = name_csv_file.format(step).format('_',now.strftime("%m_%d"))
                    to_file = rf'{path_csv_file}/{name_csv_file_new}'
                    df.to_csv(to_file, index=False, sep=',', encoding='utf-8')
                    print(f'DONE {now}')

                    n += 1
                    my_connect.close()
                    step += 1
                    sleep(20)
                else:
                    step += 1

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
            df.to_csv(to_file, index=False, sep=',', encoding='utf-8')
            print(f'DONE {now}')

            n += 1
            my_connect.close()
            sleep(20)