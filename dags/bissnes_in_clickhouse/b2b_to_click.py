def b2b():
    import pandas as pd
    import pymysql
    from clickhouse_driver import Client

    queue_list = [9262]

    user='robot_read_only'
    password='du9Itg5bnzTb'


    servers = ['192.168.1.15',
                '192.168.1.81',
                '192.168.1.36',
                '192.168.1.84',
                '192.168.1.85',
                '192.168.1.123',
                '192.168.1.103',
                '192.168.1.86',
                '192.168.1.122',
                '192.168.1.124',
                '192.168.1.125',
                '192.168.1.127',
                '192.168.1.129',
                '192.168.1.126',
                '192.168.1.130',
                '192.168.1.87',
                '192.168.1.59',
                '192.168.1.114',
                '192.168.1.146',
                '192.168.1.109',
                '192.168.1.107',
                '192.168.1.156',
                '192.168.1.110',
                '192.168.1.113',
                '192.168.1.118',
                '192.168.1.116',
                '192.168.1.147']


    no_data_rollback = pd.DataFrame()
    k = 0

    df_full = pd.DataFrame()
    for server in servers:
        try:
            connection = pymysql.connect(host= server,
                                user=user,
                                password=password,
                                database='robot'
                                ,cursorclass=pymysql.cursors.DictCursor
                                        )
            with connection.cursor() as cursor:
                
                for queue in queue_list:
                    
                    sql_show = f'''SHOW TABLES
                    FROM robot
                    like 'cel_just-call-{queue}%' 
                    '''
                    sql_show = sql_show.replace('\n','')
                    df = pd.read_sql_query(sql_show, connection).drop_duplicates()
                    
                    if df.shape[0] > 0:
                    
                        sql = f'''select distinct phone, 'B2B' mark
                            from (
                                    select distinct phone,
                                                    `set`
                            from `cel_just-call-{queue}-new_1` a
                                    left join `bill_cdr` b on a.uniqueid = b.uniqueid
                        where date(date) = date(now()) - interval 1 day
                            and `set` like '%БИЗНЕС%') tt
                        '''
                        cursor.execute(sql)
                        result = cursor.fetchone()
                        df = pd.read_sql_query(sql, connection)

                        df_full = df_full.append(df)

                        k += 1
                    
                    else:
                        print(f'This server ({server}) does not have table {queue}')
                        pass
                    
        except:
            print(f'server {server} Error')
            pass


    df_full['phone'] = df_full['phone'].apply(lambda x: x.replace('.0',''))
    df_full=df_full.astype('str')
    print('Подключаемся к clickhouse')
    dest = '/root/airflow/dags/not_share/ClickHouse198.csv'
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
        # return host, user, password

    try:
         
        client = Client(host=host, user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})
        print(df_full.shape[0])
    
        client.insert_dataframe('INSERT INTO suitecrm_robot_ch.b2b_marker VALUES', df_full)
        
    except (ValueError):
        print('Данные не загружены')
    finally:

        client.connection.disconnect()
        print('conection closed')

    