# Выгрузка данных с сервера с помощью sql запросов

def sql_query_to_csv(cloud, path_sql_file, path_csv_file, date_f, name_csv_file, current_separator=';'):
        
        import pymysql
        import pandas as pd

        from commons.connect_db import connect_db


        host, user, password = connect_db(cloud)
        my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                 db="suitecrm",
                                 charset='utf8')

        my_query = open(path_sql_file).read()

        df = pd.read_sql_query(my_query.format(date_f=date_f.astype(str)), my_connect)
        print(name_csv_file,' sql request ', my_query.format(date_f=date_f))
        print(name_csv_file,' size ',df.shape[0])

        to_file = rf'{path_csv_file}{name_csv_file}'
        df.to_csv(to_file, index=False, sep=current_separator, encoding='utf-8')
        print('save to ', to_file)

        my_connect.close()

def sql_query_to_csv_log(cloud, path_sql_file, path_csv_file, i_month, date_f, name_csv_file, current_separator=';'):
        
        import pymysql
        import pandas as pd

        from commons.connect_db import connect_db

        file_name = 'suitecrm_robot.jc_robot_log_{}'.format(i_month)

        host, user, password = connect_db(cloud)
        my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                 db="suitecrm",
                                 charset='utf8')

        my_query = open(path_sql_file).read()
        print( name_csv_file,' sql request ', my_query.format(file_name=file_name, i_month=date_f))

        df = pd.read_sql_query(my_query.format(file_name=file_name, i_month=date_f), my_connect)
        
        print(name_csv_file,' size ',df.shape[0])
        to_file = rf'{path_csv_file}{name_csv_file}'
        df.to_csv(to_file, index=False, sep=current_separator, encoding='utf-8')
        print('save to ', to_file)

        my_connect.close()