# Та же функция отправки sql скрипта на сервер, но с циклом в несколько раз

def repeat_download(n,days,cloud, path_sql_file, path_csv_file, name_csv_file):
    import pymysql
    import pandas as pd
    import datetime
    from time import sleep

    from commons.connect_db import connect_db
    
    for i in range(0,days):
        host, user, password = connect_db(cloud)
        my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                     db="suitecrm",
                                     charset='utf8')

        my_query = open(path_sql_file).read().replace('п»ї','').replace('﻿','').replace('\ufeff','').format(n)
        print(my_query)

        df = pd.read_sql_query(my_query, my_connect)
        now = datetime.datetime.now() - datetime.timedelta(days=n)

        name_csv_file_new = name_csv_file.format(now.strftime("%m_%d"))

        to_file = rf'{path_csv_file}/{name_csv_file_new}'
        df.to_csv(to_file, index=False, sep=',', encoding='utf-8')
        print(f'DONE {now}')

        n += 1
        my_connect.close()
        sleep(20)

def sql_query_to_csv(cloud, path_sql_file, path_csv_file, name_csv_file, current_separator=','):
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