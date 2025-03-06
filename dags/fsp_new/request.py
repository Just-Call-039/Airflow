import pandas as pd
from fsp_new import def_project
from commons_liza import load_mysql, dbs



def get_request_data(request_sql, cloud, steps_sql, dialogi_sql, team_sql, voronka_sql,
                      konva_path, konva_group_path, to_dbs_konva, to_dbs_konva_group):



    team_project = def_project.team_project()
    team_project['date'] = team_project['date'].astype('str')
    team_project['team'] = team_project['team'].astype('str')

    sql_steps = open(steps_sql, 'r', encoding='utf8').read()

    
    print('steps')
    
    steps = load_mysql.get_data_request(sql_steps, cloud)
    steps2 = def_project.step_perevod()
    steps = str(list(dict.fromkeys(steps.step.to_list()+steps2.step.to_list()))).strip('[]')

    
    sql_dialogi = open(dialogi_sql, 'r', encoding='utf-8').read().format(steps)
    print(sql_dialogi)

    sql_part1 = open(request_sql, 'r', encoding='utf-8').read()

    
    print('dialogi')
    
    dialogi = load_mysql.get_data_request(sql_dialogi, cloud)
    dialogi['datecall'] = pd.to_datetime(dialogi['datecall'])
    dialogi['last_step'] = dialogi['last_step'].fillna(0).astype('int').astype('str')
    dialogi['queue'] = dialogi['queue'].fillna(0).astype('int').astype('str')

    dialogi = dialogi.merge(steps2, how = 'left', left_on = ['queue','last_step','datecall'], right_on = ['ochered', 'step', 'date']).fillna('0').query('step != "0"')
    dialogi['RN'] = dialogi.sort_values(['datecall'], ascending=[True]).groupby(
        ['phone', 'assigned_user_id']).cumcount() + 1
    dialogi = dialogi[dialogi['RN'] == 1]

    print('meetings')
    

    part1 = load_mysql.get_data_request(sql_part1, cloud)
    team_sql = open(team_sql, 'r', encoding='utf-8').read()
    teams = load_mysql.get_data_request(team_sql, cloud).fillna('')

    part1 = part1.merge(teams, how = 'left', left_on = 'uid', right_on = 'id')

    def team_y(row):
        if row['team_x'] == '':
            return row['team_y']
        else:
            return row['team_x']
            
    def city_c(row):
        if row['city_c_y'] == 0.0:
            return row['city_c_x']
        else:
            return row['city_c_y']

    part1['team'] = part1.apply(lambda row: team_y(row), axis=1)

    

    konva = part1.merge(dialogi, how='left', left_on=['uid', 'phone_work'], right_on=['assigned_user_id', 'phone'])

    konva['date'] = konva['date_entered']
    konva['datecall'] = konva['datecall'].fillna(0)
    konva['date'] = konva.apply(lambda row: def_project.date(row), axis=1)
    
    konva['region_c'] = konva['region_c'].fillna(0).astype('int')
    konva['region'] = konva['region_c'].astype('str')
    konva['ptv_c'] = konva['ptv_c'].fillna('^0^')
    konva['region'] = konva.apply(lambda row: def_project.region(row), axis=1)
    konva = konva.fillna(0)
    konva['city_c'] = konva.apply(lambda row: city_c(row), axis=1)

    konva[['destination_queue', 'queue', 'last_queue_c']] = konva[['destination_queue', 'queue', 'last_queue_c']].fillna(0)
    konva['queue_c'] = konva.apply(lambda row: def_project.queue(row), axis=1)
    konva['destination_queue_c'] = konva.apply(lambda row: def_project.destination_queue(row), axis=1)

    konva['date'] = konva['date'].astype('str').apply(lambda x: x.replace(' 00:00:00',''))
    konva['team'] = konva['team'].astype('str')
    konva = konva.merge(team_project, how='left', on=['date', 'team'])
    konva['team_project'] = konva['team_project'].fillna('0')

    konva['check_team_project'] = konva.apply(lambda row: def_project.check_team_project(row), axis=1)

    konva['project'] = konva.apply(lambda row: def_project.meet_proect_final(row), axis=1)

  
    konva['organization'] = konva.apply(lambda row: def_project.organization(row), axis=1)


    sql_log = open(voronka_sql, 'r', encoding='utf8').read()

    log = load_mysql.get_data_request(sql_log, cloud).drop_duplicates()

    log_full = log.merge(konva, how='left', left_on='parent_id', right_on='rtkid')[['parent_id', 'date_created', 'status_before', 'status_after',
        'date_before', 'date_after', 'project', 'team', 'uid', 'fio_x', 'date', 'date_entered', 'status', 'konva', 'tarif',
                                'marker', 'last_step', 'region','region_c2', 'queue_c', 'destination_queue_c',
                                'network_provider','organization','directory','trunk_id',
                                'city_c']].rename(
        columns={'date': 'calldate',
                'fio_x': 'fio',
                'queue_c': 'queue',
                'destination_queue_c': 'destination_queue',
                'project': 'proect'})
    log_full['rn'] = log_full.sort_values('date_created').groupby(['parent_id']).cumcount() + 1
    log_path = '/root/airflow/dags/liza_test/Files/Воронка рекрутинга.csv'
    log_full.to_csv(log_path, index=False)
    dbs_log = 'scripts fsp\Current Files\Воронка рекрутинга.csv'

    dbs.save_file_to_dbs(log_path, dbs_log)


    konva_group = konva.groupby(['project', 'team', 'uid', 'fio_x', 'date', 'date_entered', 'status', 'konva', 'tarif',
                                'department', 'marker', 'last_step', 'region','region_c2', 'queue_c', 'destination_queue_c',
                                'network_provider','organization','directory','inbound_call',
                                'city_c','trunk_id'], as_index=False, dropna=False).agg({'rtkid': 'count'}).rename(
        columns={'rtkid': 'vsego',
                'date': 'calldate',
                'fio_x': 'fio',
                'queue_c': 'queue',
                'destination_queue_c': 'destination_queue',
                'project': 'proect'})
    
    
    konva.to_csv(konva_path, index=False) 
    konva_group.to_csv(konva_group_path, index=False)
    dbs.save_file_to_dbs(konva_path, to_dbs_konva)
    dbs.save_file_to_dbs(konva_group_path, to_dbs_konva_group)
