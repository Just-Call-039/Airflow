# Функция для преобразования запроса operator_calls.

def operator_calls_transformation(n, days, files_from_sql, main_folder, path_to_users, name_users):
    import pandas as pd
    import glob
    import os
    import fsp.def_project_definition as def_project_definition

    print('Пользователи')
    teams = pd.read_csv(f'{path_to_users}/{name_users}').fillna('')
    teams = teams[['id', 'team']]

    team_project = def_project_definition.team_project()
    queue_project = def_project_definition.queue_project()
    team_project['date'] = team_project['date'].astype('str')
    team_project['team'] = team_project['team'].astype('str')
    queue_project['date'] = queue_project['date'].astype('str')
    queue_project['Очередь'] = queue_project['Очередь'].astype('str')

    print('Функция команды')

    def def_team_y(row):
        if row['team_x'] == '':
            return row['team_y']
        else:
            return row['team_x']

    print('Обработка файлов')
    print(f'Всего файлов {len(os.listdir(files_from_sql))}')

    n -= 1
    for i in range(0,days):
        files = glob.glob(files_from_sql + "/*.csv")

        print(f'Текущий файл # {n+1}')
        print(os.listdir(files_from_sql)[n])
        calls = pd.read_csv(files[n])

        calls = calls.merge(teams, how='left', left_on='assigned_user_id', right_on='id')

        calls['team'] = calls.apply(lambda row: def_team_y(row), axis=1)

        calls['datec'] = calls['datec'].astype('str')
        calls['team'] = calls['team'].astype('str')
        calls['queue'] = calls['queue'].astype('str')

        calls = calls.merge(team_project, how='left', left_on=['datec', 'team'], right_on=['date', 'team'])
        calls = calls.merge(queue_project, how='left', left_on=['datec', 'queue'], right_on=['date', 'Очередь'])

        calls['destination_project'] = calls['destination_project'].fillna('0')
        calls['team_project'] = calls['team_project'].fillna('0')

        calls['project'] = calls.apply(lambda row: def_project_definition.project(row), axis=1)

        calls['organization'] = calls.apply(lambda row: def_project_definition.organization(row), axis=1)

        calls = calls.groupby(['assigned_user_id',
                                'datec',
                                'queue',
                                'destination_queue',
                                'project',
                                'network_provider_c',
                                'city_c',
                                'region',
                                'data_type',
                                'department',
                                'marker',
                                'organization',
                                'inbound_call',
                                'directory',
                                'last_step'], as_index=False, dropna=False).agg({'id_x': 'count'}).rename(
            columns={'region': 'region_c', 'id_x': 'count'})
        calls['summ_by'] = calls.groupby(['assigned_user_id', 'datec'], as_index=False)['count'].transform('sum')

        print('Сохраняем файл')
        calls.to_csv(f'{main_folder}/{os.listdir(files_from_sql)[n]}', sep=',', index=False, encoding='utf-8')

        n += 1
