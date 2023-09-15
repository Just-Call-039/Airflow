def robotlog_calls_transformation(n, days, files_from_sql, main_folder, path_to_users, name_users):
    import pandas as pd
    import os
    import glob
    import datetime
    import fsp.def_project_definition as def_project_definition

    print('Пользователи')
    teams = pd.read_csv(f'{path_to_users}/{name_users}').fillna('')
    teams = teams[['id', 'team']]
    teams['team'] = teams['team'].apply(lambda x: x.strip())

    print('Функция команды')
    def def_team_y(row):
        if row['team_x'] == '':
            return row['team_y']
        else:
            return row['team_x']

    team_project = def_project_definition.team_project()
    queue_project = def_project_definition.queue_project()
    team_project['date'] = team_project['date'].astype('str')
    team_project['team'] = team_project['team'].astype('str')
    queue_project['date'] = queue_project['date'].astype('str')
    queue_project['Очередь'] = queue_project['Очередь'].astype('str')

    print('Обработка файлов')
    print(f'Всего файлов {len(os.listdir(files_from_sql))}')

    n -= 1
    for i in range(0,days):
        files = glob.glob(files_from_sql + "/*.csv")

        print(f'Текущий файл # {n+1}')
        print(os.listdir(files_from_sql)[n])
        trafic = pd.read_csv(files[n], sep=',')
        print(trafic.columns)

        trafic = trafic.merge(teams, how='left', left_on='assigned_user_id', right_on='id')

        print(trafic.columns)

        trafic['team'] = trafic.apply(lambda row: def_team_y(row), axis=1)

        trafic['call_date'] = trafic['call_date'].astype('str')
        trafic['team'] = trafic['team'].astype('str').apply(lambda x: x.strip())
        trafic['queue'] = trafic['queue'].astype('str').apply(lambda x: x.strip())

        trafic = trafic.merge(team_project, how='left', left_on=['call_date', 'team'], right_on=['date', 'team'])
        trafic = trafic.merge(queue_project, how='left', left_on=['call_date', 'queue'], right_on=['date', 'Очередь'])

        trafic['destination_project'] = trafic['destination_project'].fillna('0')
        trafic['team_project'] = trafic['team_project'].fillna('0')

        trafic['project'] = trafic.apply(lambda row: def_project_definition.project(row), axis=1)
        trafic['organization'] = trafic.apply(lambda row: def_project_definition.organization(row), axis=1)

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
                                'trunk_id',
                                'marker',
                                'organization',
                                'inbound_call',
                                'directory',
                                'last_step'], as_index=False, dropna = False).agg({'perevod': 'sum',
                                                                    'id_x': 'count',
                                                                    'billsec': 'sum',
                                                                    'real_billsec': 'sum'}) \
            .rename(columns={'call_date': 'date(call_date)',
                            'region_c': 'region',
                            'id_x': 'count(id)',
                            'billsec': 'tr_ro',
                            'real_billsec': 'tr_pay'})

        print('Сохраняем файл')
        trafic.to_csv(f'{main_folder}/{os.listdir(files_from_sql)[n]}', sep=',', index=False, encoding='utf-8')

        n += 1

