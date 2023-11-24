# Функция для извлечения хоста, логина, пароля.
# Необходимо передать файл с соответствующим наименованием.
# Maria_db, 72, Combat, Click, Server_MySQL, DBS, cloud_117, cloud_128.


def connect_db(file):
    dest = None
    if file == 'Maria_db':
        dest = '/root/airflow/dags/not_share/Maria_db.csv'
    elif file == 'cloud_117':
        dest = '/root/airflow/dags/not_share/cloud_my_sql_117.csv'
    elif file == 'cloud_128':
        dest = '/root/airflow/dags/not_share/cloud_my_sql_128.csv'
    elif file == 'cloud_182':
        dest = '/root/airflow/dags/not_share/cloud_my_sql_182.csv'
    elif file == 'cloud_183':
        dest = '/root/airflow/dags/not_share/cloud_my_sql_183.csv'
    elif file == '72':
        dest = '/root/airflow/dags/not_share/Second_cloud_72.csv'
    elif file == 'Combat':
        dest = '/root/airflow/dags/not_share/Combat_server.csv'
    elif file == 'Click':
        dest = '/root/airflow/dags/not_share/ClickHouse.csv'
    # elif file == 'Click2':
    #     dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
    elif file == 'Server_MySQL':
        dest = '/root/airflow/dags/not_share/Server_files_MySQL.csv'
    elif file == 'DBS':
        dest = '/root/airflow/dags/not_share/DBS.csv'
    else:
        print('Неизвестный сервер.')

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
        return host, user, password
