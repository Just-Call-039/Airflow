def load_expect(cloud, path_sql_file, path_csv_file, name_csv_file, n, current_separator=','):
    import pymysql
    import pandas as pd

    from commons_sawa.connect_db import connect_db
    print('try read file cloud ', cloud)
    
    host, user, password = connect_db(cloud)
    print('try connection')
    my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                 db="suitecrm",
                                 charset='utf8')

    my_query = open(path_sql_file, "r", encoding='utf8', errors='ignore').read().format(n=n).replace('п»ї','').replace('﻿','').replace('\ufeff','')
    print(my_query)
    df = pd.read_sql_query(my_query, my_connect)

    to_file = rf'{path_csv_file}/{name_csv_file}'
    df.to_csv(to_file, index=False, sep=current_separator, encoding='utf-8')

    my_connect.close()
