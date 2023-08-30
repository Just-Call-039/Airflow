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
                            phone_work         String,
                            pr1          Nullable(String),
                            pr2          Nullable(String)

                        ) ENGINE = TinyLog'''
    client.execute(sql_create)

    print('ЧАСТЬ ПЕРВАЯ ___________________________________________')
    print('Выгружаем Нашу разметку')

    sql = '''select phone_work,
                trim(BOTH ',' FROM replace(replace(concat(toString(ttk_pr),',',toString(mts_pr),',',toString(rtk_pr),',',toString(nbn_pr),',',toString(dom_pr),',',toString(bln_pr)),'0,',''),'0','')) list,
                    trim(BOTH ',' FROM replace(replace(concat(toString(ttk_p),',',toString(mts_p),',',toString(rtk_p),',',toString(nbn_p),',',toString(dom_p),',',toString(bln_p)),'0,','') ,'0','')) list2
                    from
                (select phone_work,
                        if(ptv_ttk = 1, ttk, 0) ttk_pr,
                    if(ptv_mts = 1, mts, 0) mts_pr,
                    if(ptv_rtk = 1, rtk, 0) rtk_pr,
                    if(ptv_nbn = 1, nbn, 0) nbn_pr,
                    if(ptv_dom = 1, dom, 0) dom_pr,
                    if(ptv_bln = 1, bln, 0) bln_pr,
                    if(ptv_ttk = 1, 'ttk', '0') ttk_p,
                    if(ptv_mts = 1, 'mts', '0') mts_p,
                    if(ptv_rtk = 1, 'rtk', '0') rtk_p,
                    if(ptv_nbn = 1, 'nbn', '0') nbn_p,
                    if(ptv_dom = 1, 'dom', '0') dom_p,
                    if(ptv_bln = 1, 'bln', '0') bln_p

                from (select *, if(ptv_rtk + ptv_ttk + ptv_mts + ptv_nbn + ptv_dom + ptv_bln > 0, 1, 0) as ptv_nasha
                    from suitecrm_robot_ch.contacts_temp) as contacts
                        left join (select *, 1 as ptv_nasha from suitecrm_robot_ch.priority_providers) priority
                                on town_c = code_region and provider = network_provider and contacts.ptv_nasha = priority.ptv_nasha
                where ptv_nasha = 1                ) tt
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

    df['pr1'] = df.apply(lambda row: priority1(row), axis=1)
    df['pr2'] = df.apply(lambda row: priority2(row), axis=1)

    bln = df[(df['pr1'] == 'bln') | (df['pr2'] == 'bln')].phone_work.count()
    mts = df[(df['pr1'] == 'mts') | (df['pr2'] == 'mts')].phone_work.count()
    ttk = df[(df['pr1'] == 'ttk') | (df['pr2'] == 'ttk')].phone_work.count()
    nbn = df[(df['pr1'] == 'nbn') | (df['pr2'] == 'nbn')].phone_work.count()
    dom = df[(df['pr1'] == 'dom') | (df['pr2'] == 'dom')].phone_work.count()
    rtk = df[(df['pr1'] == 'rtk') | (df['pr2'] == 'rtk')].phone_work.count()

    print('Разметка Наша по приоритетам')
    print('')
    print(f'Билайн {bln:,}'.replace(',', ' '))
    print(f'МТС {mts:,}'.replace(',', ' '))
    print(f'ТТК {ttk:,}'.replace(',', ' '))
    print(f'НБН {nbn:,}'.replace(',', ' '))
    print(f'ДомРу {dom:,}'.replace(',', ' '))
    print(f'РТК {rtk:,}'.replace(',', ' '))


    print('Заливаем в итоговую таблицу')
    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.contacts VALUES', df[['phone_work','pr1','pr2']])






    # print('ЧАСТЬ ВТОРАЯ ___________________________________________')
    # print('Выгружаем Нашу разметку')

    # sql = '''select phone_work,
    #                 trim(BOTH ',' FROM replace(replace(concat(toString(ttk_pr), ',', toString(mts_pr), ',', toString(rtk_pr), ',',
    #                                                             toString(nbn_pr), ',', toString(dom_pr), ',', toString(bln_pr)), '0,',
    #                                                     ''), '0', '')) list,
    #                 trim(BOTH ',' FROM replace(replace(concat(toString(ttk_p), ',', toString(mts_p), ',', toString(rtk_p), ',',
    #                                                             toString(nbn_p), ',', toString(dom_p), ',', toString(bln_p)), '0,',
    #                                                     ''), '0', '')) list2
    #             from (select phone_work,
    #                         if(ptv_fias_ttk = 1, ttk, 0)     ttk_pr,
    #                         if(ptv_fias_mts = 1, mts, 0)     mts_pr,
    #                         if(ptv_fias_rtk = 1, rtk, 0)     rtk_pr,
    #                         if(ptv_fias_nbn = 1, nbn, 0)     nbn_pr,
    #                         if(ptv_fias_dom = 1, dom, 0)     dom_pr,
    #                         if(ptv_fias_bln = 1, bln, 0)     bln_pr,
    #                         if(ptv_fias_ttk = 1, 'ttk', '0') ttk_p,
    #                         if(ptv_fias_mts = 1, 'mts', '0') mts_p,
    #                         if(ptv_fias_rtk = 1, 'rtk', '0') rtk_p,
    #                         if(ptv_fias_nbn = 1, 'nbn', '0') nbn_p,
    #                         if(ptv_fias_dom = 1, 'dom', '0') dom_p,
    #                         if(ptv_fias_bln = 1, 'bln', '0') bln_p

    #                 from (select *,
    #                             case when (ptv_rtk + ptv_ttk + ptv_mts + ptv_nbn + ptv_dom + ptv_bln) > 1 then 0
    #                             when (ptv_fias_ttk + ptv_fias_rtk + ptv_fias_mts + ptv_fias_bln + ptv_fias_nbn + ptv_fias_dom) > 0 then 1 else 0 end ptv_ne_nasha
    #                         from suitecrm_robot_ch.contacts_temp) as contact
    #                         left join (select *, 1 as ptv_ne_nasha from suitecrm_robot_ch.priority_providers) priority
    #                                     on town_c = code_region and provider = network_provider
    #                                             and contact.ptv_ne_nasha = priority.ptv_ne_nasha
    #                 where contact.ptv_ne_nasha = 1
    #                     ) tt
    #             '''
    
    # df = pd.DataFrame(client.query_dataframe(sql))

    # def priority1(row):
    #     X = row['list2'].split(',')
    #     Y = row['list'].split(',')
    #     pr = [x for _,x in sorted(zip(Y,X))][0:1]
    #     pr = str(pr).replace('[','').replace(']','').replace("'",'')
    #     return pr

    # def priority2(row):
    #     X = row['list2'].split(',')
    #     Y = row['list'].split(',')
    #     pr2 = [x for _,x in sorted(zip(Y,X))][1:2]
    #     pr2 = str(pr2).replace('[','').replace(']','').replace("'",'')
    #     return pr2
    
    # print('Проставляем приоритеты')

    # df['pr1'] = df.apply(lambda row: priority1(row), axis=1)
    # df['pr2'] = df.apply(lambda row: priority2(row), axis=1)

    # bln = df[(df['pr1'] == 'bln') | (df['pr2'] == 'bln')].phone_work.count()
    # mts = df[(df['pr1'] == 'mts') | (df['pr2'] == 'mts')].phone_work.count()
    # ttk = df[(df['pr1'] == 'ttk') | (df['pr2'] == 'ttk')].phone_work.count()
    # nbn = df[(df['pr1'] == 'nbn') | (df['pr2'] == 'nbn')].phone_work.count()
    # dom = df[(df['pr1'] == 'dom') | (df['pr2'] == 'dom')].phone_work.count()
    # rtk = df[(df['pr1'] == 'rtk') | (df['pr2'] == 'rtk')].phone_work.count()

    # print('Всего разметки по приоритетам')
    # print('')
    # print(f'Билайн {bln:,}'.replace(',', ' '))
    # print(f'МТС {mts:,}'.replace(',', ' '))
    # print(f'ТТК {ttk:,}'.replace(',', ' '))
    # print(f'НБН {nbn:,}'.replace(',', ' '))
    # print(f'ДомРу {dom:,}'.replace(',', ' '))
    # print(f'РТК {rtk:,}'.replace(',', ' '))


    # print('Заливаем в итоговую таблицу')
    # client = Client(host=host, port='9000', user=user, password=password,
    #                 database='suitecrm_robot_ch', settings={'use_numpy': True})

    # client.insert_dataframe('INSERT INTO suitecrm_robot_ch.contacts VALUES', df[['phone_work','pr1','pr2']])
