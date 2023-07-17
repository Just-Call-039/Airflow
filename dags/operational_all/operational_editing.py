def operational_transformation(path_to_users, name_users, path_to_folder, name_calls, path_to_final_folder):
    import pandas as pd
    import glob
    import os
    import datetime
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


    now = datetime.datetime.now()
    df = pd.read_csv(f'{path_to_folder}/{name_calls}')

    print('Соединяем и выводим итоговый проект')
    df['dialog'] = df['dialog'].astype('str')
    df['team'] = df['team'].astype('str')
    df['calldate'] = df['calldate'].astype('str')
    df = df.merge(team_project, how='left', left_on=['calldate', 'team'], right_on=['date', 'team'])
    df = df.merge(queue_project, how='left', left_on=['calldate', 'dialog'], right_on=['date', 'Очередь'])
    df['destination_project'] = df['destination_project'].fillna('0')
    df['team_project'] = df['team_project'].fillna('0')

    df['project'] = df.apply(lambda row: def_project_definition.project(row), axis=1)
    df['category'] = ''

    print('Группируем')
    df = df.groupby(['project',
        'dialog',
        'destination_queue',
        'calldate',
        'client_status',
        'was_repeat',
        'marker',
        'route',
        'source',
        'perevod',
        'region',
        'holod',
        'city_c',
        'otkaz',
        'trunk_id',
        'autootvet',
        'category_stat',
        'stretched',
        'category',
        'last_step'], as_index=False, dropna=False).agg({'calls': 'sum','trafic': 'sum',})

    print('Сохраняем')

    df.to_csv(f'{path_to_final_folder}/{name_calls}', sep=',', index=False, encoding='utf-8')

