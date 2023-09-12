def setting_priorities():
    import pandas as pd
    import pymysql
    from clickhouse_driver import Client

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


    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})
    
    print('Удаляем приоритетную таблицу и пересоздаем')
    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    sql_drop = '''drop table suitecrm_robot_ch.contacts'''
    client.execute(sql_drop)

    sql_create = '''create table suitecrm_robot_ch.contacts
                        (
                            id_custom         String,
                            ptv          Nullable(Int8),
                            priority1          Nullable(String),
                            priority2          Nullable(String)

                        ) ENGINE = TinyLog'''
    client.execute(sql_create)

    print('ЧАСТЬ ПЕРВАЯ ___________________________________________')
    print('Выгружаем всю разметку')

    sql = '''select id_custom,
       ptv,
       ifNull(trim(BOTH ',' FROM replace(replace(concat(toString(ttk_pr), ',', toString(mts_pr), ',', toString(rtk_pr), ',',
                                                 toString(nbn_pr), ',', toString(dom_pr), ',', toString(bln_pr)), '0,',
                                          ''), '0', '')),'') list,
       ifNull(trim(BOTH ',' FROM replace(replace(concat(toString(ttk_p), ',', toString(mts_p), ',', toString(rtk_p), ',',
                                                 toString(nbn_p), ',', toString(dom_p), ',', toString(bln_p)), '0,',
                                          ''), '0', '')),'') list2
        from (select id as id_custom,
             ptv,
             case when ptv_ttk = 1 then ttk when ptv = 2 and ptv_fias_ttk = 1 then ttk else 0 end ttk_pr,
             case when ptv_mts = 1 then mts when ptv = 2 and ptv_fias_mts = 1 then mts else 0 end mts_pr,
             case when ptv_rtk = 1 then rtk when ptv = 2 and ptv_fias_rtk = 1 then rtk else 0 end rtk_pr,
             case when ptv_nbn = 1 then nbn when ptv = 2 and ptv_fias_nbn = 1 then nbn else 0 end nbn_pr,
             case when ptv_dom = 1 then dom when ptv = 2 and ptv_fias_dom = 1 then dom else 0 end dom_pr,
             case when ptv_bln = 1 then bln when ptv = 2 and ptv_fias_bln = 1 then bln else 0 end bln_pr,

             case
                 when ptv_ttk = 1 and ttk is not null then 'ttk'
                 when ptv = 2 and ptv_fias_ttk = 1 and ttk is not null then 'ttk'
                 else '0' end                                                                     ttk_p,
             case
                 when ptv_mts = 1 and mts is not null then 'mts'
                 when ptv = 2 and ptv_fias_mts = 1 and mts is not null then 'mts'
                 else '0' end                                                                     mts_p,
             case
                 when ptv_rtk = 1 and rtk is not null then 'rtk'
                 when ptv = 2 and ptv_fias_rtk = 1 and rtk is not null then 'rtk'
                 else '0' end                                                                     rtk_p,
             case
                 when ptv_nbn = 1 and nbn is not null then 'nbn'
                 when ptv = 2 and ptv_fias_nbn = 1 and nbn is not null then 'nbn'
                 else '0' end                                                                     nbn_p,
             case
                 when ptv_dom = 1 and dom is not null then 'dom'
                 when ptv = 2 and ptv_fias_dom = 1 and dom is not null then 'dom'
                 else '0' end                                                                     dom_p,
             case
                 when ptv_bln = 1 and bln is not null then 'bln'
                 when ptv = 2 and ptv_fias_bln = 1 and bln is not null then 'bln'
                 else '0' end                                                                     bln_p
      from suitecrm_robot_ch.contacts_temp
               left join suitecrm_robot_ch.priority_providers on priority_providers.city_c = contacts_temp.city_c
          and provider = network_provider
          and contacts_temp.ptv = priority_providers.ptv_c
          and contacts_temp.region_c = priority_providers.region_c
      where ptv != 3
    ) tt
                '''
    
    df = pd.DataFrame(client.query_dataframe(sql))

    def priority1(row):
        X = row['list2'].split(',')
        Y = row['list'].split(',')
        pr = [x for _,x in sorted(zip(Y,X))][0:1]
        pr = str(pr).replace('[','').replace(']','').replace("'",'')
        return pr

    def priority2(row):
        X = row['list2'].split(',')
        Y = row['list'].split(',')
        pr2 = [x for _,x in sorted(zip(Y,X))][1:2]
        pr2 = str(pr2).replace('[','').replace(']','').replace("'",'')
        return pr2

    print('Проставляем приоритеты')
    print(1)
    df['priority1'] = df.apply(lambda row: priority1(row), axis=1)
    print(2)
    df['priority2'] = df.apply(lambda row: priority2(row), axis=1)

    bln = df[(df['ptv'] == 1) & ((df['priority1'] == 'bln') | (df['priority2'] == 'bln'))].id_custom.count()
    mts = df[(df['ptv'] == 1) & ((df['priority1'] == 'mts') | (df['priority2'] == 'mts'))].id_custom.count()
    ttk = df[(df['ptv'] == 1) & ((df['priority1'] == 'ttk') | (df['priority2'] == 'ttk'))].id_custom.count()
    nbn = df[(df['ptv'] == 1) & ((df['priority1'] == 'nbn') | (df['priority2'] == 'nbn'))].id_custom.count()
    dom = df[(df['ptv'] == 1) & ((df['priority1'] == 'dom') | (df['priority2'] == 'dom'))].id_custom.count()
    rtk = df[(df['ptv'] == 1) & ((df['priority1'] == 'rtk') | (df['priority2'] == 'rtk'))].id_custom.count()

    bln2 = df[(df['ptv'] == 2) & ((df['priority1'] == 'bln') | (df['priority2'] == 'bln'))].id_custom.count()
    mts2 = df[(df['ptv'] == 2) & ((df['priority1'] == 'mts') | (df['priority2'] == 'mts'))].id_custom.count()
    ttk2 = df[(df['ptv'] == 2) & ((df['priority1'] == 'ttk') | (df['priority2'] == 'ttk'))].id_custom.count()
    nbn2 = df[(df['ptv'] == 2) & ((df['priority1'] == 'nbn') | (df['priority2'] == 'nbn'))].id_custom.count()
    dom2 = df[(df['ptv'] == 2) & ((df['priority1'] == 'dom') | (df['priority2'] == 'dom'))].id_custom.count()
    rtk2 = df[(df['ptv'] == 2) & ((df['priority1'] == 'rtk') | (df['priority2'] == 'rtk'))].id_custom.count()

    print('Разметка Наша по приоритетам')
    print('')
    print(f'Билайн {bln:,}'.replace(',', ' '))
    print(f'МТС {mts:,}'.replace(',', ' '))
    print(f'ТТК {ttk:,}'.replace(',', ' '))
    print(f'НБН {nbn:,}'.replace(',', ' '))
    print(f'ДомРу {dom:,}'.replace(',', ' '))
    print(f'РТК {rtk:,}'.replace(',', ' '))
    print('')
    print('')
    print('Разметка Не наша по приоритетам')
    print('')
    print(f'Билайн {bln2:,}'.replace(',', ' '))
    print(f'МТС {mts2:,}'.replace(',', ' '))
    print(f'ТТК {ttk2:,}'.replace(',', ' '))
    print(f'НБН {nbn2:,}'.replace(',', ' '))
    print(f'ДомРу {dom2:,}'.replace(',', ' '))
    print(f'РТК {rtk2:,}'.replace(',', ' '))

    df2 = df[df['priority1'] != ''][['id_custom',
                               'ptv',
                               'priority1','priority2']]


    print('Заливаем в итоговую таблицу')
    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.contacts VALUES', df2)