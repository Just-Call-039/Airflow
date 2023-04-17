# Функция для выполнения SQL запроса и записи в файл.
# Необходимо передать наименование облака, полный путь и имя SQL файла, путь к csv файлу без последней /, имя csv файла.


def sql_query_to_csv(cloud, path_sql_file, path_csv_file, name_csv_file, current_separator=';'):
    import pymysql
    import pandas as pd

    from commons.connect_db import connect_db

    host, user, password = connect_db(cloud)
    my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                 db="suitecrm",
                                 charset='utf8')

    my_query = open(path_sql_file).read()

    df = pd.read_sql_query(my_query, my_connect)

    to_file = rf'{path_csv_file}/{name_csv_file}'
    df.to_csv(to_file, index=False, sep=current_separator, encoding='utf-8')
    
    my_connect.close()
