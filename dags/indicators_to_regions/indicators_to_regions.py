from datetime import timedelta
from datetime import datetime
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from fsp.repeat_download import sql_query_to_csv
from commons.transfer_file_to_dbs import transfer_file_to_dbs
from indicators_to_regions.regions_editer import region_editer
from indicators_to_regions import to_clickhous, download_from_dbs, clear_folder
from indicators_to_regions import download_from_dbs

default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='indicators_to_regions',
    schedule_interval='50 4 * * *',
    start_date=pendulum.datetime(2023, 8, 24, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

path_to_sql = '/root/airflow/dags/indicators_to_regions/SQL/'
sql_request = f'{path_to_sql}Request.sql'
sql_calls = f'{path_to_sql}Call.sql'
sql_transfer = f'{path_to_sql}TransferRobot.sql'

path_to_files = '/root/airflow/dags/indicators_to_regions/Files/'
path_to_file_sql = f'{path_to_files}sql_files/'
path_to_file_sql_calls = f'{path_to_files}sql_files/callls/'
path_to_file = f'{path_to_files}total/'
# path_to_decoding = f'{path_to_files}decoding.xlsx'
path_to_transfer_sql = f'{path_to_files}transfer/'
path_to_user = '/root/airflow/dags/request_with_calls_today/Files/users.csv'
path_to_request = '/root/airflow/dags/request_with_calls_today/Files/sql_total/request.csv'
path_to_decoding = '/root/airflow/dags/current_month_yesterday/Files/decoding.xlsx'
path_to_workhour = '/root/airflow/dags/current_month_yesterday/Files/4/working_time_current_month.csv'
path_to_workprevios = '/root/airflow/dags/previos_month/Files/working/'

csv_request = 'request.csv'
csv_request_result = 'Requests .csv'
csv_transfer = 'Transfer текущий.csv'
csv_calls2 = 'calls 01_2023.csv'
csv_calls = 'calls текущий.csv'
csv_result = 'CallsTotal текущий.csv'
csv_result2 = 'CallsTotal архив 03-05_2024.csv'
csv_city = 'Город.csv'

file_name_city = 'Город.csv'


dbs_result1 = '/Отчеты BI/Показатели до регионов/Заявки/'
dbs_result2 = '/Отчеты BI/Показатели до регионов/CallsTotal/'
dbs_result_transfer = '/Отчеты BI/Показатели до регионов/TransferRobot/'
path_to_dbs = '/Отчеты BI/Стандартные справочники/'
path_to_calls_dbs = '/Отчеты BI/Показатели до регионов/calls/'
path_to_transfer_dbs = '/Отчеты BI/Показатели до регионов/transfer/'



# Блок выполнения SQL запросов.
request_sql = PythonOperator(
    task_id='request_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_request, 'path_csv_file': path_to_file_sql, 'name_csv_file': csv_request}, 
    dag=dag
    )

calls_sql = PythonOperator(
    task_id='calls_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_calls, 'path_csv_file': path_to_file_sql_calls, 'name_csv_file': csv_calls}, 
    dag=dag
    )
transfer_sql = PythonOperator(
    task_id='transfer_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_transfer, 'path_csv_file': path_to_transfer_sql, 'name_csv_file': csv_transfer}, 
    dag=dag
    )

download_calls_arhiv = PythonOperator(
    task_id='download_calls_arhive', 
    python_callable=download_from_dbs.transfer_file_from_dbs, 
    op_kwargs={'file_path_on_share' : path_to_calls_dbs,\
                'local_file_path' : path_to_file_sql_calls,\
                'file_name_list' : ['calls 04_2024.csv', 'calls 05_2024.csv', 'calls 06_2024.csv', 'calls 07_2024.csv', 'calls 08_2024.csv']}, 
    dag=dag
    )

download_transfer_arhiv = PythonOperator(
    task_id='download_transfer_arhive', 
    python_callable=download_from_dbs.transfer_file_from_dbs, 
    op_kwargs={'file_path_on_share' : path_to_transfer_dbs,\
                'local_file_path' : path_to_transfer_sql,\
                'file_name_list' : ['Transfer 42024.csv', 'Transfer 52024.csv', 'Transfer 62024.csv', 'Transfer 72024.csv', 'Transfer 82024.csv']}, 
    dag=dag
    )


download_city = PythonOperator(
    task_id = 'download_city',
    python_callable=download_from_dbs.transfer_file_from_dbs,
    op_kwargs={'file_path_on_share' : f'{path_to_dbs}{file_name_city}',
                'local_file_path' : f'{path_to_files}{file_name_city}'},
    dag=dag
    )

# Преобразование файлов после sql.
region_editing = PythonOperator(
    task_id='region_editing', 
    python_callable=region_editer, 
    op_kwargs={'path_to_files': path_to_file_sql,'requests': csv_request,'calls': csv_calls, 'path_result': path_to_file,
                  'file_result_req': csv_request_result,'file_result': csv_result}, 
    dag=dag
    )

# Отправка в кликхаус
df_to_click = PythonOperator(
    task_id='to_click', 
    python_callable=to_clickhous.to_click, 
    op_kwargs={'path_to_files' : path_to_files, 
               'csv_city' : csv_city, 
               'path_to_request' : path_to_request, 
               'path_to_call' : path_to_file_sql_calls, 
               'path_to_transfer' : path_to_transfer_sql, 
               'path_to_user' : path_to_user, 
               'path_to_decoding' : path_to_decoding, 
               'path_to_workhour' : path_to_workhour, 
               'path_to_workprevios' : path_to_workprevios
            }, 
    dag=dag
    )


calls_to_dbs = PythonOperator(
    task_id='calls_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file, 'to_path': dbs_result2, 'file': csv_result, 'db': 'DBS'}, 
    dag=dag
    )
request_to_dbs = PythonOperator(
    task_id='request_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file, 'to_path': dbs_result1, 'file': csv_request_result, 'db': 'DBS'}, 
    dag=dag
    )
transfer_to_dbs = PythonOperator(
    task_id='transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_transfer_sql, 'to_path': dbs_result_transfer, 'file': csv_transfer, 'db': 'DBS'}, 
    dag=dag
    )

clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder.clear_folder, 
    op_kwargs={'folder_list': [path_to_transfer_sql, path_to_file_sql, path_to_file]}, 
    dag=dag
    )

path_to_transfer_sql



[request_sql, calls_sql, transfer_sql, download_city, download_calls_arhiv, download_transfer_arhiv] \
    >> region_editing >> [calls_to_dbs, request_to_dbs, transfer_to_dbs] >> clear_folders

[request_sql, calls_sql, transfer_sql, download_city, download_calls_arhiv, download_transfer_arhiv] >> df_to_click >> clear_folders