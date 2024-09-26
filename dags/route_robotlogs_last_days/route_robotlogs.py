import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from route_robotlogs_last_days import download_from_dbs
from route_robotlogs_last_days import save_to_dbs
from route_robotlogs_last_days import processing
from route_robotlogs_last_days import clear_folder

default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='route_last_days',
    schedule_interval='0 5 * * *',
    start_date=pendulum.datetime(2023, 7, 25, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud_name = 'cloud_128'

# Наименование файлов

file_name_conv = 'Конвертация.csv'
file_name_operator = 'Операторы.csv'
file_name_quality = 'Качество.csv'
file_name_cities = 'Город.csv'
file_name_hours = 'Часы.csv'
file_name_request = 'Заявки.csv'

file_name_ro_xlsx = 'Типы РО.xlsx'
file_name_ro_csv = 'Типы РО.csv'

file_name_project_xlsx = 'Проекты.xlsx'
file_name_project_csv = 'Проекты.csv'

# Пути к sql запросам на сервере airflow

path_to_sql_airflow = '/root/airflow/dags/route_robotlogs_last_days/SQL/'

sql_log = f'{path_to_sql_airflow}robot_log.sql'
sql_dest = f'{path_to_sql_airflow}destination.sql'
sql_city = f'{path_to_sql_airflow}callcity.sql'

# Пути к файлам этого проекта на сервере airflow

path_project_folder = '/root/airflow/dags/route_robotlogs_during_day/Files/'

path_to_robot_log = f'{path_project_folder}robot_log/'
path_to_route = f'{path_project_folder}route/'
path_to_route_unique = f'{path_project_folder}route_unique/'

# Пути к другим файлам на сервере airflow
path_to_folder = '/root/airflow/dags/project_defenition/projects/'
path_to_team = f'{path_to_folder}teams/'
path_to_step = f'{path_to_folder}steps/'
path_to_queue = f'{path_to_folder}queues/'

#  Пути к файлам в папке dbs

path_to_dbs = 'scripts fsp/Current Files/'
name_dbs_operators = 'Пользователи.csv'
name_dbs_hours = 'Ч.csv'

path_to_dbs_1 = '/Отчеты BI/Стандартные справочники/'

name_dbs_ro = 'Группировка очередей.xlsx'
name_dbs_project = 'Календарь.xlsx'

# Пути к файлам проекта на dbs 

path_project_dbs = 'scripts fsp/Current Files/route_robotlog_during_day/'
path_dbs_log = f'{path_project_dbs}robot_log/'
path_dbs_route = f'{path_project_dbs}route/'
path_dbs_route_unique = f'{path_project_dbs}route_unique/'

# Выполнение заданий

# Загрузка датасетов

download_operators = PythonOperator(
    task_id='download_operators',
    python_callable=download_from_dbs.transfer_file_from_dbs,
    op_kwargs={'file_path_on_share' : f'{path_to_dbs}{name_dbs_operators}',\
                'local_file_path' : f'{path_project_folder}{file_name_operator}'},
    dag=dag
    )

# Обработка данных, все мерджи и группировка

group_of_log = PythonOperator(
    task_id='group_log',
    python_callable=processing.group_log,
    op_kwargs={'path_project_folder' : path_project_folder,
            'path_sql_log' : sql_log,
            'path_sql_dest' : sql_dest,
            'path_sql_city' : sql_city,
            'path_to_robot_log' : path_to_robot_log,
            'path_to_defenition' : path_to_folder,
            'file_name_operator' : file_name_operator,
            'path_to_route' : path_to_route,
            'path_to_route_unique' : path_to_route_unique},
    dag=dag
)

# Блок отправки датасетов с маршруатми в папку DBS

transfer_route = PythonOperator(
    task_id='transfer_route', 
    python_callable=save_to_dbs.save_to_dbs, 
    op_kwargs={'path_from': path_to_route, 'path_to': path_dbs_route}, 
    dag=dag
    )

transfer_unique_route = PythonOperator(
    task_id='transfer_unique_route', 
    python_callable=save_to_dbs.save_to_dbs, 
    op_kwargs={'path_from': path_to_route_unique, 'path_to': path_dbs_route_unique}, 
    dag=dag
    )

# Отправка датасетов с логами

transfer_robotlog = PythonOperator(
    task_id='transfer_robotlog', 
    python_callable=save_to_dbs.save_to_dbs, 
    op_kwargs={'path_from': path_to_robot_log, 'path_to': path_dbs_log}, 
    dag=dag
    )


# Очистка папки

clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder.clear_folder, 
    op_kwargs={'folder': path_project_folder}, 
    dag=dag
    )

download_operators >> group_of_log  >> [transfer_route, transfer_unique_route, transfer_robotlog] >> clear_folders


