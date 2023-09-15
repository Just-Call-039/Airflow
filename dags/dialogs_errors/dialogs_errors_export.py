def dialog_errors():
    import pandas as pd
    import pymysql
    from clickhouse_driver import Client
    from urllib.parse import quote_plus
    from sqlalchemy.engine import create_engine
    import time
    

    print('Подключаемся к mysql')
    dest = '/root/airflow/dags/not_share/clouds_mysql_ro_cells.csv'
    if dest:
        with open(dest) as file:
            for now in file:
                now = now.strip().split('=')
                first, second = now[0].strip(), now[1].strip()
                if first == 'servers':
                    servers = second.split(',')
                elif first == 'user':
                    user = second
                elif first == 'password':
                    password = second



    data = pd.DataFrame()
    n = 0
    print('Start export')

    for server in servers:
        try:    
        
            n += 1
            m = server.replace('84.201.','').replace('192.168.','')
            print(f'    server {n}')
            sql_show = '''SHOW TABLES
                            FROM robot'''
            sql_show = sql_show.replace('\n','')
            # engine = create_engine("mysql://user:user@server/robot" % quote_plus("password"))
            Con = pymysql.Connect(host=server, user=user, passwd=password, db='robot', charset='utf8')
            df = pd.read_sql_query(sql_show, Con).drop_duplicates()
            df.columns = ['tables']
            tables = df.tables.to_list()

            if 'dialogs_errors' in tables:
                sql = f'''SELECT *, '{m}' server_number
                FROM dialogs_errors
                where date(date) = date(now()) 
                # - interval 1 day
                '''
                sql = sql.replace('\n','')


                Con = pymysql.Connect(host=server, user=user, passwd=password, db='robot', charset='utf8')
                df = pd.read_sql_query(sql, Con).drop_duplicates()

                data = pd.concat([data,df], axis = 0)

                Con.close()
                
                
        except:
            time.sleep(30)

            try:
                print(f'    server {n} try again {m}')

                sql_show = '''SHOW TABLES
                                FROM robot'''
                sql_show = sql_show.replace('\n','')

                Con = pymysql.Connect(host=server, user=user, passwd=password, db='robot', charset='utf8')
                df = pd.read_sql_query(sql_show, Con).drop_duplicates()
                df.columns = ['tables']
                tables = df.tables.to_list()

                if 'dialogs_errors' in tables:
                    sql = f'''SELECT *, '{m}' server_number
                    FROM dialogs_errors
                    where date(date) = date(now()) - interval 1 day
                    '''
                    sql = sql.replace('\n','')


                    Con = pymysql.Connect(host=server, user=user, passwd=password, db='robot', charset='utf8')
                    df = pd.read_sql_query(sql, Con).drop_duplicates()

                    data = pd.concat([data,df], axis = 0)

                    Con.close()
            except:        
                print(f'    server {n} Error {m}')
                pass

    data[['id', 'clid', 'uniqueid', 'dialog_name', 'type', 'error','parsed', 'server_number']] = data[['id', 'clid', 'uniqueid', 'dialog_name', 'type', 'error','parsed', 'server_number']].astype('str')



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

    print('Заливаем данные')
    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.dialogs_errors VALUES', data)





# drop table suitecrm_robot_ch.dialogs_errors;

# create table suitecrm_robot_ch.dialogs_errors
#                     (
#                         id Nullable(String),
#                         clid String,
#                         uniqueid Nullable(String),
#                         dialog_name Nullable(String),
#                         date Nullable(DateTime),
#                         type Nullable(String),
#                         error Nullable(String),
#                         parsed Nullable(String),
#                         server_number Nullable(String)

#                     ) ENGINE = MergeTree
# order by clid