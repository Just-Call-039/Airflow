def lids_editing (waiters,lids,dop,path_to_file,file, path_file):
    import pandas as pd
    from datetime import timedelta
    from oauth2client.service_account import ServiceAccountCredentials
    from datetime import datetime, timedelta
    from datetime import datetime
    from clickhouse_driver import Client


    
    waiters = pd.read_csv(f'{path_to_file}/{waiters}').fillna('')
    lids = pd.read_csv(f'{path_to_file}/{lids}').fillna('')
    dop = pd.read_csv(f'{path_to_file}/{dop}').fillna('')

    lids['type'] ='Лид'
    waiters['type'] ='Ждун'

    
    tab = pd.read_csv('/root/airflow/dags/incoming_traffic/Files/Лидовые шаги.csv',  sep=';', encoding='utf-8').fillna('')

    tab['Шаг'] = tab['Шаг'].astype('str').apply(lambda x: x.replace('.0',''))
    tab['Очередь'] = tab['Очередь'].astype('str').apply(lambda x: x.replace('.0',''))
    lids['dialog'] = lids['dialog'].astype('str').apply(lambda x: x.replace('.0',''))
    lids['last_step'] = lids['last_step'].astype('str').apply(lambda x: x.replace('.0',''))



    df = lids.merge(tab, left_on = ['dialog','last_step'], right_on = ['Очередь','Шаг'], how = 'inner').fillna('')
    

    waiters = waiters.astype('str')


    df = df[['phone','calldate','dialog','town_c','last_step','ptv','type']].astype('str')
    tt = pd.concat([waiters, df, dop])

    tt['phone'] = tt['phone'].astype('str').apply(lambda x: x.replace('.0',''))
    tt['dialog'] = tt['dialog'].astype('str').apply(lambda x: x.replace('.0',''))
    tt['last_step'] = tt['last_step'].astype('str').apply(lambda x: x.replace('.0',''))
    tt['town_c'] = tt['town_c'].astype('str').apply(lambda x: x.replace('.0',''))
    tt['last_step'] = tt['last_step'].astype('str').apply(lambda x: x.replace('.0',''))
    tt = tt.merge(tab, left_on = ['dialog','last_step'], right_on = ['Очередь','Шаг'], how = 'left').fillna('')
    tt = tt[['phone','calldate','dialog','town_c','last_step','ptv','type','Группировка']].astype('str')

    tt.to_csv(rf'{path_file}/{file}', index=False, sep=',', encoding='utf-8')
