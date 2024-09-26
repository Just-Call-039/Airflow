import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from commons import transfer_file_to_dbs

from route_robotlogs_during_day import download_from_dbs
from route_robotlogs_during_day import save_to_dbs
from route_robotlogs_during_day import processing
from route_robotlogs_during_day import clear_folder
from route_robotlogs_during_day import download_google

default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='route_during_day',
    schedule_interval='0 8,11,14,17 * * *',
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
file_name_route = 'route_today.csv'
file_name_unique_route = 'route_unique_today.csv'
file_name_log = 'robot_log_today.csv'
file_name_city = 'city_today.csv'
file_name_dest = 'dest_today.csv'

file_name_ro_xlsx = 'Типы РО.xlsx'
file_name_ro_csv = 'Типы РО.csv'

file_name_project_xlsx = 'Проекты.xlsx'
file_name_project_csv = 'Проекты.csv'

name_df_step = 'steps/steps_today.csv'
name_df_team = 'team_today.csv'
name_df_queue = 'queue_today.csv'

# Пути к sql запросам на сервере airflow

path_to_sql_airflow = '/root/airflow/dags/route_robotlogs_during_day/SQL/'

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

path_to_step = f'{path_to_folder}steps/'


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

download_queue = PythonOperator(
    task_id='download_queue',
    python_callable=download_google.project_queues,
    op_kwargs={'path_to_folder' : path_project_folder, 'file_name' : name_df_queue},
    dag=dag
    )

download_team = PythonOperator(
    task_id='download_team',
    python_callable=download_google.project_teams,
    op_kwargs={'path_to_folder' : path_project_folder, 'file_name' : name_df_team},
    dag=dag
    )

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
            'name_df_step' : name_df_step,
            'name_df_team' : name_df_team,
            'name_df_queue' : name_df_queue,
            'file_name_operator' : file_name_operator,
            'file_name_log' : file_name_log,
            'file_name_city' : file_name_city,
            'file_name_dest' : file_name_dest,
            'path_to_route' : path_to_route,
            'path_to_route_unique' : path_to_route_unique,
            'file_name_route' : file_name_route,
            'file_name_route_unique' : file_name_unique_route},
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

#  Блок загрузки и обработки дополнительных таблиц для power bi

download_quality = PythonOperator(
    task_id='download_quality',
    python_callable=download_from_dbs.transfer_file_from_dbs,
    op_kwargs={'file_path_on_share' : f'{path_to_dbs_1}{file_name_quality}',\
                'local_file_path' : f'{path_project_folder}{file_name_quality}'},
    dag=dag
    )

processing_quality = PythonOperator(
    task_id='processing_quality',
    python_callable=processing.processing_quality,
    op_kwargs={'file_name_quality' : f'{path_project_folder}{file_name_quality}'},
    dag=dag
)

download_ro = PythonOperator(
    task_id='download_ro',
    python_callable=download_from_dbs.transfer_file_from_dbs,
    op_kwargs={'file_path_on_share' : f'{path_to_dbs_1}{name_dbs_ro}',\
                'local_file_path' : f'{path_project_folder}{file_name_ro_xlsx}'},
    dag=dag
    )

processing_ro = PythonOperator(
    task_id='processing_ro',
    python_callable=processing.xlsx_to_csv,
    op_kwargs={'file_xlsx' : f'{path_project_folder}{file_name_ro_xlsx}',\
                'file_csv' : f'{path_project_folder}{file_name_ro_csv}', 'sheet_name' : 'Лист1'},
    dag=dag
    )

convert_ro = PythonOperator(
    task_id='convert_ro',
    python_callable=processing.convert_ro,
    op_kwargs={'path_to_folder' : path_project_folder, 'file_name' : file_name_ro_csv},
    dag=dag
)

download_project = PythonOperator(
    task_id='download_project',
    python_callable=download_from_dbs.transfer_file_from_dbs,
    op_kwargs={'file_path_on_share' : f'{path_to_dbs_1}{name_dbs_project}',\
               'local_file_path' : f'{path_project_folder}{file_name_project_xlsx}'},
    dag=dag
)

processing_project = PythonOperator(
    task_id='processing_project',
    python_callable=processing.xlsx_to_csv,
    op_kwargs={'file_xlsx' : f'{path_project_folder}{file_name_project_xlsx}',\
                'file_csv' : f'{path_project_folder}{file_name_project_csv}', 'sheet_name' : 'Лист3'},
    dag=dag
    )

download_cities = PythonOperator(
    task_id='download_cities',
    python_callable=download_from_dbs.transfer_file_from_dbs,
    op_kwargs={'file_path_on_share' : f'{path_to_dbs_1}{file_name_cities}',\
                'local_file_path' : f'{path_project_folder}{file_name_cities}'},
    dag=dag
    )

processing_cities = PythonOperator(
    task_id='processing_cities',
    python_callable=processing.processing_cities,
    op_kwargs={'file_name_cities' : f'{path_project_folder}{file_name_cities}'},
    dag=dag
)

download_hours = PythonOperator(
    task_id='download_hours',
    python_callable=download_from_dbs.transfer_file_from_dbs,
    op_kwargs={'file_path_on_share' : f'{path_to_dbs}{name_dbs_hours}',\
                'local_file_path' : f'{path_project_folder}{file_name_hours}'},
    dag=dag
    )

processing_hours = PythonOperator(
    task_id='processing_hours',
    python_callable=processing.processing_hours,
    op_kwargs={'file_name_hours' : f'{path_project_folder}{file_name_hours}'},
    dag=dag
)



download_request = PythonOperator(
    task_id='download_request',
    python_callable=download_from_dbs.transfer_file_from_dbs,
    op_kwargs={'file_path_on_share' : f'{path_to_dbs}{file_name_request}',\
                'local_file_path' : f'{path_project_folder}{file_name_request}'},
    dag=dag
    )

create_conv = PythonOperator(
    task_id='create_conv',
    python_callable=processing.create_conv,
    op_kwargs={'path_to_folder' : path_project_folder,
                'file_name_request' : file_name_request,
                'file_name_conv' : file_name_conv},
    dag=dag
)

#  Блок загрузки таблиц конверсия, адреса и тд на dbs

transfer_quality = PythonOperator(
    task_id='transfer_quality', 
    python_callable=transfer_file_to_dbs.transfer_file_to_dbs, 
    op_kwargs={'from_path': path_project_folder, 'to_path': path_project_dbs, 'file': file_name_quality, 'db': 'DBS'}, 
    dag=dag
    )

transfer_cities = PythonOperator(
    task_id='transfer_cities', 
    python_callable=transfer_file_to_dbs.transfer_file_to_dbs, 
    op_kwargs={'from_path': path_project_folder, 'to_path': path_project_dbs, 'file': file_name_cities, 'db': 'DBS'}, 
    dag=dag
    )

transfer_hours = PythonOperator(
    task_id='transfer_hours', 
    python_callable=transfer_file_to_dbs.transfer_file_to_dbs, 
    op_kwargs={'from_path': path_project_folder, 'to_path': path_project_dbs, 'file': file_name_hours, 'db': 'DBS'}, 
    dag=dag
    )

transfer_conv = PythonOperator(
    task_id='transfer_conversion', 
    python_callable=transfer_file_to_dbs.transfer_file_to_dbs, 
    op_kwargs={'from_path': path_project_folder, 'to_path': path_project_dbs, 'file': file_name_conv, 'db': 'DBS'}, 
    dag=dag
    )

transfer_ro = PythonOperator(
    task_id='transfer_ro', 
    python_callable=transfer_file_to_dbs.transfer_file_to_dbs, 
    op_kwargs={'from_path': path_project_folder, 'to_path': path_project_dbs, 'file': file_name_ro_csv, 'db': 'DBS'}, 
    dag=dag
    )

transfer_project = PythonOperator(
    task_id='transfer_project', 
    python_callable=transfer_file_to_dbs.transfer_file_to_dbs, 
    op_kwargs={'from_path': path_project_folder, 'to_path': path_project_dbs, 'file': file_name_project_csv, 'db': 'DBS'}, 
    dag=dag
    )

# Очистка папки

clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder.clear_folder, 
    op_kwargs={'folder': path_project_folder}, 
    dag=dag
    )




[download_queue, download_team, download_operators] >> group_of_log  >> [transfer_route, transfer_unique_route, transfer_robotlog] 
download_quality >> processing_quality >> transfer_quality
download_cities >> processing_cities >> transfer_cities
download_hours >> processing_hours >> transfer_hours
download_request >> create_conv >> transfer_conv 
download_ro >> processing_ro >> convert_ro >> transfer_ro
download_project >> processing_project >> transfer_project
[transfer_ro, transfer_project, transfer_route, transfer_unique_route, transfer_robotlog, transfer_conv, transfer_quality, transfer_cities, transfer_hours] >> clear_folders



