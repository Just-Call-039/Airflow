# Выгрузка датасета sql скриптом из базы
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

# Та же функция отправки sql скрипта на сервер, но с циклом в несколько раз, где n отсрочка дней, а days - количество повторов
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

# Так же функция отправки sql скрипта на сервер, но с циклом по лимиту строк, где x - начальная строка, y - количество строк, count_repeats - количество повторов
def repeat_download_data(x, y, count_repeats, limit_list, cloud, path_sql_file, path_csv_file, name_csv_file):
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

        df = pd.read_sql_query(my_query, my_connect)
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
            sleep(20)
        elif x > limit_list[-1]:
            print(f'Идет сохранение файла Data_{x}')
            way = rf'{path_csv_file}/{name_csv_file}'
            data.to_csv(way.format(x), index=False, sep=',', encoding='utf-8')
            del data
            data = pd.DataFrame()
            print('Готово')
            print(datetime.today().strftime("%m/%d/%Y, %H:%M:%S"))
            # my_connect.close()
            sleep(20)

        my_connect.close()
        sleep(20)