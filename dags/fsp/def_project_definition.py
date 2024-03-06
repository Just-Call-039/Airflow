import pandas as pd
import glob
import os


def team_project():
    path = '/root/airflow/dags/project_defenition/projects/teams'
    files = glob.glob(path + "/*.csv")
    project_team = pd.DataFrame()
    n = 0
    num_of_files = len(os.listdir(path))
    print(f'Всего файлов {num_of_files}')

    for i in files:
        n += 1
        df = pd.read_csv(i)
        project_team = pd.concat([project_team,df], axis = 0)
    del df


    project_team['team'] = project_team['team'].astype('str').apply(lambda x: x.replace('я',''))
    project_team['team'] = project_team['team'].astype('str').apply(lambda x: x.replace('.0',''))
    project_team['team'] = project_team['team'].astype('str').apply(lambda x: x.replace('nan','0'))

    project_team['team'] = project_team['team'].fillna('0').astype('int').astype('str')
    project_team = project_team.rename(columns={'project': 'team_project'})
    project_team['date'] = project_team['date'].astype('str')
    project_team['team'] = project_team['team'].astype('str')

    project_team = project_team.reset_index(drop=True)
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
        project_queue = pd.concat([project_queue,df], axis = 0)
    del df

    project_queue = project_queue.rename(columns={'Проект (набирающая очередь)': 'destination_project'})
    project_queue['destination_project'] = project_queue['destination_project'].fillna('DR')
    project_queue['Очередь'] = project_queue['Очередь'].fillna(0).astype('int').astype('str')
    project_queue['date'] = project_queue['date'].astype('str')

    project_queue = project_queue.reset_index(drop=True)
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
    elif row['queue'] in [9251,9251.0,'9251','9251.0']:
        return 'Just Job'
    else:
        return 'КЦ'
    
def queue_project2():
    path = '/root/airflow/dags/project_defenition/projects/queues'
    files = sorted(glob.glob(path + "/*.csv"), reverse=True)
    project_queue = pd.DataFrame()
    n = 0
    num_of_files = len(os.listdir(path))

    print(f'Всего файлов {num_of_files}')

    for i in files:
        n += 1
        df = pd.read_csv(i)
        project_queue = pd.concat([project_queue, df], ignore_index=True, axis=0)
        # project_queue = project_queue.append(df)
    del df

    # print(project_queue.columns)

    project_queue = project_queue.rename(columns={'Группировка': 'type_ro'})
    project_queue['date'] = project_queue['date'].astype('str')

    project_queue['RN'] = project_queue.groupby(['Очередь', 'date']).cumcount() + 1
    project_queue = project_queue[project_queue['RN'] == 1][['Очередь', 'type_ro', 'date']]

    return project_queue

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
        steps = steps.append(df)
    del df

    steps['step'] = steps['step'].fillna(0).astype('int').astype('str')
    steps['ochered'] = steps['ochered'].fillna(0).astype('int').astype('str')
    steps['date'] = pd.to_datetime(steps['date'])
    # steps['step'] = steps['step'].astype('int')

    steps['RN'] = steps.groupby(['step', 'ochered', 'date']).cumcount() + 1
    steps = steps[steps['RN'] == 1][['step', 'ochered', 'date']]

    return steps