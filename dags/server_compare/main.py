import datetime
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from server_compare import defs, to_click, proccessing
from route_robotlogs.download_from_dbs import transfer_file_from_dbs
from route_robotlogs.clear_folder import clear_folder



default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='server_compare',
    schedule_interval='0 4 * * *',
    start_date=pendulum.datetime(2023, 7, 25, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud = 'cloud_128'
# cloud = 'cloud_183'

date_i = datetime.date.today() - datetime.timedelta(days=1)

# Определим написание даты в названиях файлов

name_date_csv = f'{date_i.year}_{date_i.month:02}_{date_i.day:02}'

# Путь к запросам к базе Mysql

robotlog_request_path = '/root/airflow/dags/server_compare/SQL/robot_log.sql'
debug_parse_request_path = '/root/airflow/dags/server_compare/SQL/debug_parse.sql'


# Путь к запросу к Clickhouse а создание и сохранение данных

clickhouse_save_request = '/root/airflow/dags/server_compare/SQL/save_server.sql'

# Наименование путей к папкам проекта

file_path = '/root/airflow/dags/server_compare/Files/'

# Наименование файлов

robotlog_filename = f'robotlog_{name_date_csv}.csv'
debug_parse_filename = f'debug_parse_{name_date_csv}.csv'

# Наименование путей к папке с очередями, шагами и командами

project_defenition_path = '/root/airflow/dags/project_defenition/projects/'

# Файлы шаги, команды и очереди

step_file = f'{project_defenition_path}/steps/steps_{name_date_csv}.csv'
queue_file = f'{project_defenition_path}/queues/queues_{name_date_csv}.csv'
team_file = f'{project_defenition_path}/teams/teams_{name_date_csv}.csv'

# Путь к файлам с городами и пользователями в даге current_month

current_month_path = '/root/airflow/dags/current_month_yesterday/Files/'

city_file = f'{current_month_path}Город.csv'
quality_file = f'{current_month_path}Качество.csv'

# Наименование путей к папке dbs 

dbs_path = 'scripts fsp/Current Files/'

# Наименование файла с операторами

user_filename = 'Пользователи.csv'

# Название таблицы роботлог на Clickhouse 

robotlog_table_clickhouse = 'server_compare'

type_dict = {
             'last_step' : 'str', 
            'dialog' : 'str',
            'server_number' : 'str',
            'autootvetchik' : 'int8',
            'client_status' : 'str',
            'directory' : 'str',
            'phone' : 'str',
            'found' : 'float64',
            'search_sec' : 'float64',
            'sqltook_sec' : 'float64',
            'marker' : 'str',
            'real_billsec' : 'int64',
            'trunk_id' : 'str',
            'network_provider_c' : 'str',
            'city' : 'str',
            'town' : 'str',
            'perevod' : 'int8',
            'perevod_done' : 'int8',
            'request' : 'int8',
            'project' : 'str',
            'quality' : 'str'}



# Загружаем robotlog с базы

get_robotlog = PythonOperator(
    task_id = 'get_robotlog',
    python_callable = defs.get_data_mysql,
    op_kwargs = {'path_sql_file' : robotlog_request_path,
                'cloud' : cloud, 
                'date_i' : date_i, 
                'path_to_file' : file_path, 
                'file_name' : robotlog_filename},
    dag = dag
    )

# Загружаем debug_parse с базы

get_debug_parse = PythonOperator(
    task_id = 'get_debug_parse',
    python_callable = defs.get_data_mysql,
    op_kwargs = {'path_sql_file' : debug_parse_request_path,
                'cloud' : cloud, 
                'date_i' : date_i, 
                'path_to_file' : file_path, 
                'file_name' : debug_parse_filename},
    dag = dag
    )

# Загружаем датасет пользователи с пакпи dbs

get_user = PythonOperator(
    task_id = 'get_user',
    python_callable = transfer_file_from_dbs,
    op_kwargs = {'file_path_on_share' : f'{dbs_path}{user_filename}',
                'local_file_path' : f'{file_path}{user_filename}'},
    dag = dag
    )

# Подготавливаем датасет для загрузки в CH

main_proccess = PythonOperator(
    task_id = 'main_proccess',
    python_callable = proccessing.proccess_server_df,
    op_kwargs = {'file_path' : file_path, 
                 'robotlog_filename' : robotlog_filename, 
                 'debug_parse_filename' : debug_parse_filename,
                 'step_file' : step_file, 
                 'queue_file' : queue_file, 
                 'user_filename' : user_filename, 
                 'team_file' : team_file,
                 'quality_file' : quality_file, 
                 'city_file' : city_file},
    dag = dag
    )

# Отправляем данные в Clickhouse

save_robotlog_chlickhouse = PythonOperator(
    task_id = 'save_robotlog_chlickhouse',
    python_callable = to_click.save_table,
    op_kwargs = {
                'table_name' : robotlog_table_clickhouse,
                'save_sql' : clickhouse_save_request,
                'result_path' : f'{file_path}{robotlog_filename}',
                'type_dict' : type_dict},
    dag = dag
    )

# Очистка папки

clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder, 
    op_kwargs={'folder': file_path}, 
    dag=dag
    )


get_robotlog >> get_debug_parse >> get_user >> main_proccess >> save_robotlog_chlickhouse >> clear_folders


