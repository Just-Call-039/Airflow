import pandas as pd
import datetime
from fsp_new import def_project
from commons_liza import load_mysql, dbs


def get_trafic(trafic_sql, cloud, trafic_path, trafic_dbs_path, team_sql, n, days):

    trafic_sql = open(trafic_sql, 'r', encoding='utf-8').read()
    team_sql = open(team_sql, 'r', encoding='utf-8').read()

    for i in range(0,days):

        now = datetime.datetime.now() - datetime.timedelta(days=n)
        print(f'Текущий день {now.strftime("%m_%d")}')
        
        team_project = def_project.team_project()
        queue_project = def_project.queue_project()
        step_perevod = def_project.step_perevod()

        team_project['date'] = team_project['date'].astype('str')
        team_project['team'] = team_project['team'].astype('str')
        queue_project['date'] = queue_project['date'].astype('str')
        queue_project['Очередь'] = queue_project['Очередь'].astype('str')
        step_perevod['step'] = step_perevod['step'].astype('str')
        step_perevod['ochered'] = step_perevod['ochered'].astype('str')
        step_perevod['type_steps'] = step_perevod['type_steps'].astype('int').astype('str')
        step_perevod['date'] = step_perevod['date'].astype('str')
        step_perevod = step_perevod.rename(columns={'ochered': 'queue', 'date': 'call_date'})

        print('Коннект')

        print('Отправляем запрос')
        # trafic_sql = open(trafic_sql, 'r', encoding='utf-8').read().format(n = n)
        trafic = load_mysql.get_data_request(trafic_sql.format(n = n), cloud)
        
        if trafic.shape[0] == 0:
            print('pass dataframe')
            pass
        else:
            trafic['call_date'] = pd.to_datetime(trafic['call_date']).astype('str')
            trafic = trafic.merge(step_perevod, how='left', left_on=['last_step', 'queue', 'call_date'],
                                right_on=['step', 'queue', 'call_date'])
            trafic['step'] = trafic['step'].fillna('0')
            trafic['type_steps'] = trafic['type_steps'].fillna('1')
            trafic['perevod'] = trafic.apply(lambda row: def_project.perevod(row), axis=1)
            trafic['lead'] = trafic.apply(lambda row: def_project.lead(row), axis=1)
            trafic = trafic.drop(columns=['step'])
   
            teams = load_mysql.get_data_request(team_sql, cloud)
            teams = teams[['id','team']]
            teams['team'] = teams['team'].apply(lambda x: x.strip())
            trafic = trafic.merge(teams, how='left', left_on='assigned_user_id', right_on='id')

            print('Функция команды')
            def def_team_y(row):
                if row['team_x'] == '':
                    return row['team_y']
                else:
                    return row['team_x']


            trafic['team'] = trafic.apply(lambda row: def_team_y(row), axis=1)
            trafic['team'] = trafic['team'].astype('str').apply(lambda x: x.strip())
            trafic['queue'] = trafic['queue'].astype('str').apply(lambda x: x.strip())

            trafic = trafic.merge(team_project, how='left', left_on=['call_date', 'team'], right_on=['date', 'team'])
            trafic = trafic.merge(queue_project, how='left', left_on=['call_date', 'destination_queue'], right_on=['date', 'Очередь'])

            trafic['destination_project'] = trafic['destination_project'].fillna('0')
            trafic['team_project'] = trafic['team_project'].fillna('0')

            trafic['project'] = trafic.apply(lambda row: def_project.project(row), axis=1)
            trafic['organization'] = trafic.apply(lambda row: def_project.organization(row), axis=1)

            trafic = trafic.groupby(['call_date',
                                    'queue',
                                    'destination_queue',
                                    'assigned_user_id',
                                    'project',
                                    'department',
                                    'data_type',
                                    'network_provider',
                                    'city_c',
                                    'region_c',
                                    'region_c2',
                                    'trunk_id',
                                    'marker',
                                    'organization',
                                    'inbound_call',
                                    'directory',
                                    'last_step'], as_index=False, dropna = False).agg({'perevod': 'sum',
                                                                                        'lead': 'sum',
                                                                        'id_x': 'count',
                                                                        'billsec': 'sum',
                                                                        'real_billsec': 'sum'}) \
                .rename(columns={'call_date': 'date(call_date)',
                                'region_c': 'region',
                                'id_x': 'count(id)',
                                'billsec': 'tr_ro',
                                'real_billsec': 'tr_pay'})

            print('Сохраняем файл')
            trafic_path = trafic_path.format(now.strftime("%m_%d"))
            trafic.to_csv(trafic_path, index=False)

            
            to_dbs_save = trafic_dbs_path.format(now.strftime("%m_%d"))
            dbs.save_file_to_dbs(trafic_path, to_dbs_save)

        n += 1

def get_marker(marker_path, trafic_folder_path, marker_dbs_path):

    import pandas as pd
    import os

    
    files = sorted(os.listdir(trafic_folder_path), reverse=True)
    
    data = pd.read_csv(marker_path)
    
    
    n = 0
    num_of_files = len(files)

    print(f'Всего файлов {num_of_files}')

    for i in files[:2]:
        n += 1
        print(i)
        chunk = pd.read_csv(f'{trafic_folder_path}/{i}')
        chunk = chunk[['marker']].drop_duplicates().fillna(0).astype('int')
        data = pd.concat([data,chunk])
        data = data.drop_duplicates()

    data.to_csv(marker_path, index=False)
    dbs.save_file_to_dbs(marker_path, marker_dbs_path)




            