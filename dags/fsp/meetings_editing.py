def meetings_transformation(path_to_users, name_users, path_to_folder, name_calls, name_meetings, path_to_final_folder, name_phone_meetings):
    import pandas as pd
    import fsp.def_project_definition as def_project_definition
    import fsp.def_meetings_functions as def_meetings_functions

    team_project = def_project_definition.team_project()
    team_project['date'] = team_project['date'].astype('str')
    team_project['team'] = team_project['team'].astype('str')

    print('dialogi')
    dialogi = pd.read_csv(f'{path_to_folder}/{name_calls}')

    dialogi['RN'] = dialogi.sort_values(['datecall'], ascending=[True]).groupby(
        ['phone', 'assigned_user_id']).cumcount() + 1
    dialogi = dialogi[dialogi['RN'] == 1]

    print('meetings')
    part1 = pd.read_csv(f'{path_to_folder}/{name_meetings}')

    print('Пользователи')
    teams = pd.read_csv(f'{path_to_users}/{name_users}').fillna('')

    part1 = part1.merge(teams, how = 'left', left_on = 'uid', right_on = 'id')
    def team_y(row):
        if row['team_x'] == '':
            return row['team_y']
        else:
            return row['team_x']

    part1['team'] = part1.apply(lambda row: team_y(row), axis=1)

    part1['module'] = part1.apply(lambda row: def_meetings_functions.meet_proect(row), axis=1)
    part1['uid'] = part1['uid'].astype('str')
    part1['phone_work'] = part1['phone_work'].astype('str')
    dialogi['assigned_user_id'] = dialogi['assigned_user_id'].astype('str')
    dialogi['phone'] = dialogi['phone'].astype('str')

    konva = part1.merge(dialogi, how='left', left_on=['uid', 'phone_work'], right_on=['assigned_user_id', 'phone'])

    konva['date'] = konva['date_entered']
    konva['datecall'] = konva['datecall'].fillna(0)
    konva['date'] = konva.apply(lambda row: def_meetings_functions.date(row), axis=1)

    konva['region_c'] = konva['region_c'].fillna(0).astype('int')
    konva['region'] = konva['region_c'].astype('str')
    konva['ptv_c'] = konva['ptv_c'].fillna('^0^')
    konva['region'] = konva.apply(lambda row: def_meetings_functions.region(row), axis=1)

    konva[['destination_queue', 'queue', 'last_queue_c']] = konva[['destination_queue', 'queue', 'last_queue_c']].fillna(0)
    konva['queue_c'] = konva.apply(lambda row: def_meetings_functions.queue(row), axis=1)
    konva['destination_queue_c'] = konva.apply(lambda row: def_meetings_functions.destination_queue(row), axis=1)

    konva['date'] = konva['date'].astype('str')
    konva['team'] = konva['team'].astype('str')
    konva = konva.merge(team_project, how='left', on=['date', 'team'])
    konva['team_project'] = konva['team_project'].fillna('0')

    konva['check_team_project'] = konva.apply(lambda row: def_meetings_functions.check_team_project(row), axis=1)

    konva['project'] = konva.apply(lambda row: def_meetings_functions.meet_proect_final(row), axis=1)
    konva['organization'] = konva.apply(lambda row: def_project_definition.organization(row), axis=1)

    konva_group = konva.groupby(['project', 'team', 'uid', 'fio_x', 'date', 'date_entered', 'status', 'konva', 'tarif',
                                'department', 'marker', 'last_step', 'region', 'queue_c', 'destination_queue_c',
                                'network_provider','organization',
                                'city_c_x'], as_index=False, dropna=False).agg({'rtkid': 'count'}).rename(
        columns={'rtkid': 'vsego',
                'date': 'calldate',
                'fio_x': 'fio',
                'queue_c': 'queue',
                'destination_queue_c': 'destination_queue',
                'city_c_x': 'city_c',
                'project': 'proect'})

    konva_group.to_csv(f'{path_to_final_folder}/{name_meetings}', sep=',', index=False, encoding='utf-8')
    konva.to_csv(f'{path_to_final_folder}/{name_phone_meetings}', sep=',', index=False, encoding='utf-8')


# meetings_transformation(path_to_users = '/root/airflow/dags/fsp/Files/',
#                         name_users = 'users.csv',
#                         path_to_folder = '/root/airflow/dags/fsp/Files/sql_meetings/',
#                         name_calls = 'meetings_calls.csv',
#                         name_meetings = 'meetings.csv',
#                         path_to_final_folder = '/root/airflow/dags/fsp/Files/meetings/',
#                         name_phone_meetings = 'meeting_phones.csv')