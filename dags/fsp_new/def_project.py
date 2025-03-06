import pandas as pd
import os
import glob


def team_project():
    path = '/root/airflow/dags/project_defenition/projects/teams'
    

    path = '/root/airflow/dags/project_defenition/projects/teams'
    files = glob.glob(path + "/*.csv")
    project_team = pd.DataFrame()
    n = 0
    num_of_files = len(os.listdir(path))
    print(f'Всего файлов {num_of_files}')

    for i in files:
        n += 1
        df = pd.read_csv(i)
        # project_team = project_team.append(df)
        project_team = pd.concat([project_team,df])
    del df

    project_team['team'] = project_team['team'].fillna(0).astype('int').astype('str')
    project_team = project_team.rename(columns={'project': 'team_project'})
    project_team['date'] = project_team['date'].astype('str')
    project_team['team'] = project_team['team'].astype('str')

    project_team['RN'] = project_team.groupby(['team', 'date']).cumcount() + 1
    project_team = project_team[project_team['RN'] == 1][['team', 'team_project', 'date']]

    return project_team


def queue_project():
    path = '/root/airflow/dags/project_defenition/projects/queues'
    files = glob.glob(path + "/*.csv")
    project_queue = pd.DataFrame()
    n = 0
    num_of_files = len(os.listdir(path))

    print(f'Всего файлов {num_of_files}')

    for i in files:
        n += 1
        df = pd.read_csv(i)
        # project_queue = project_queue.append(df)
        project_queue = pd.concat([project_queue,df])
    del df

    project_queue = project_queue.rename(columns={'Проект (набирающая очередь)': 'destination_project'})
    project_queue['destination_project'] = project_queue['destination_project'].fillna('DR')
    project_queue['Очередь'] = project_queue['Очередь'].fillna(0).astype('int').astype('str')
    project_queue['date'] = project_queue['date'].astype('str')

    project_queue['RN'] = project_queue.groupby(['Очередь', 'date']).cumcount() + 1
    project_queue = project_queue[project_queue['RN'] == 1][['Очередь', 'destination_project', 'date']]

    return project_queue

def project(row):
    if row['team_project'] == '0' and row['destination_project'] == '0':
        return row['proect']
    elif row['team_project'] == 'DR' and row['destination_project'] == '0':
        return row['proect']
    elif row['team_project'] == '0':
        return row['destination_project']
    elif row['team_project'] == 'DR':
        return row['destination_project']
    else:
        return row['team_project']

def organization(row):
    if row['team'] in ['4','12','50','13']:
        return 'КЦ'
        # return 'Лиды'
    elif row['team'] in ['8','123']:
        return 'КЦ'
    elif ' RO' in row['project']:
        return 'Just Robots'
    elif 'LIDS' in row['project']:
        return 'Лиды'
    elif 'Job' in row['project']:
        return 'Just Job'
    # elif row['queue'] in [9251,9251.0,'9251','9251.0']:
    #     return 'Just Job'
    else:
        return 'КЦ'

def step_perevod():
    path = '/root/airflow/dags/project_defenition/projects/steps'
    files = glob.glob(path + "/*.csv")
    steps = pd.DataFrame()
    n = 0
    num_of_files = len(os.listdir(path))
    print(f'Всего файлов {num_of_files}')

    for i in files:
        n += 1
        df = pd.read_csv(i)
        # steps = steps.append(df)
        steps = pd.concat([steps,df])
    del df

    steps['step'] = steps['step'].fillna(0).astype('int').astype('str')
    steps['ochered'] = steps['ochered'].fillna(0).astype('int').astype('str')
    steps['type_steps'] = steps['type_steps'].fillna(1).astype('int').astype('str')
    # steps.loc[(steps['step'].isin(['94', '91', '93'])) & (steps['ochered'] == '9078'), 'type_steps'] = '1'
    steps['date'] = pd.to_datetime(steps['date'])
    # steps['step'] = steps['step'].astype('int')

    steps['RN'] = steps.groupby(['step', 'ochered', 'date','type_steps']).cumcount() + 1
    steps = steps[steps['RN'] == 1][['step', 'ochered', 'date','type_steps']]
    steps = steps.drop_duplicates()

    return steps

def perevod(row):
    if row['step'] == '0':
        return row['perevod']
    elif row['type_steps'] == '1':
        return 1
    else:
        return 0

def lead(row):
    if row['step'] == '0':
        return row['lead']
    elif row['type_steps'] == '0':
        return 1
    else:
        return 0


import pandas as pd

ptv_nasha = ['^5^', '^6^', '^3^', '^10^', '^11^', '^19^', ]
ptv_ne_nasha = ['^5_15^', '^5_16^', '^5_17^', '5_18^', '^5_19^', '^5_20^', '^5_21^',
                '^6_15^', '^6_16^', '^6_17^', '6_18^', '^6_19^', '^6_20^', '^6_21^',
                '^3_15^', '^3_16^', '^3_17^', '3_18^', '^3_19^', '^3_20^', '^3_21^',
                '^10_15^', '^10_16^', '^10_17^', '10_18^', '^10_19^', '^10_20^', '^10_21^',
                '^11_15^', '^11_16^', '^11_17^', '11_18^', '^11_19^', '^11_20^', '^11_21^',
                '^19_15^', '^19_16^', '^19_17^', '19_18^', '^19_19^', '^19_20^', '^19_21^']

def region(row):
    if any(w in row['ptv_c'] for w in ptv_nasha):
        return 'ptv_1'
    elif any(w in row['ptv_c'] for w in ptv_ne_nasha):
        return 'ptv_2'
    else:
        return row['region_c']
def date(row):
    if row['datecall'] == 0:
        return row['date_entered']
    return row['datecall']
def queue(row):
    if row['queue'] == 0:
        return row['last_queue_c']
    else:
        return row['queue']
def destination_queue(row):
    if row['destination_queue'] == 0:
        return row['last_queue_c']
    else:
        return row['destination_queue']
def meet_proect(row):
    if row['proect'] in {'RTK', 'RTK LIDS','TELE2'}:
        return 'RTK'
    elif row['proect'] in {'TTK', 'TTK LIDS'}:
        return 'TTK'
    elif row['proect'] in {'DOMRU', 'DOMRU LIDS', 'DOMRU Dop'}:
        return 'DOMRU'
    elif row['proect'] in {'MTS', 'MTS LIDS'}:
        return 'MTS'
    elif row['proect'] in {'GULFSTREAM','GULFSTREAM LIDS'}:
        return 'GULFSTREAM'
    elif row['proect'] in {'NBN', 'NBN LIDS'}:
        return 'NBN'
    elif row['proect'] in {'BEELINE', 'BEELINE LIDS'}:
        return 'BEELINE'
    else:
        return 'DR'
def check_team_project(row):
    if row['team_project'] in {'RTK', 'RTK LIDS'}:
        return 'RTK'
    elif row['team_project'] in {'TELE2','TELE2 LIDS'}:
        return 'TELE2'
    elif row['team_project'] in {'TTK', 'TTK LIDS'}:
        return 'TTK'
    elif row['team_project'] in {'DOMRU', 'DOMRU LIDS', 'DOMRU Dop'}:
        return 'DOMRU'
    elif row['team_project'] in {'MTS', 'MTS LIDS'}:
        return 'MTS'
    elif row['proect'] in {'GULFSTREAM','GULFSTREAM LIDS'}:
        return 'GULFSTREAM'
    elif row['team_project'] in {'NBN', 'NBN LIDS'}:
        return 'NBN'
    elif row['team_project'] in {'BEELINE', 'BEELINE LIDS'}:
        return 'BEELINE'
    else:
        return 'DR'


def meet_proect_final(row):
    if row['team_project'] == '0':
        return row['proect']
    elif row['check_team_project'] == row['module']:
        return row['team_project']
    elif row['check_team_project'] != row['module']:
        return row['module']
    elif row['check_team_project'] == row['team_project']:
        return row['team_project']
    else:
        return row['proect']