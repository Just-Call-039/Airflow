import pandas as pd
import glob
import os


def team_project():
    path = '/root/airflow/dags/Проект/Команды'
    files = glob.glob(path + "/*.csv")
    project_team = pd.DataFrame()
    n = 0
    num_of_files = len(os.listdir(path))
    print(f'Всего файлов {num_of_files}')

    for i in files:
        n += 1
        df = pd.read_csv(i)
        project_team = project_team.append(df)
    del df

    project_team['team'] = project_team['team'].fillna(0).astype('int').astype('str')
    project_team = project_team.rename(columns={'project': 'team_project'})
    project_team['date'] = project_team['date'].astype('str')
    project_team['team'] = project_team['team'].astype('str')

    project_team['RN'] = project_team.groupby(['team', 'date']).cumcount() + 1
    project_team = project_team[project_team['RN'] == 1][['team', 'team_project', 'date']]

    return project_team


def queue_project():
    path = '/root/airflow/dags/Проект/Очереди'
    files = glob.glob(path + "/*.csv")
    project_queue = pd.DataFrame()
    n = 0
    num_of_files = len(os.listdir(path))

    print(f'Всего файлов {num_of_files}')

    for i in files:
        n += 1
        df = pd.read_csv(i)
        project_queue = project_queue.append(df)
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
    if row['team'] in ['4','12','50']:
        return 'Лиды'
    elif 'LIDS' in row['team']:
        return 'Лиды'
    else:
        return 'КЦ'
