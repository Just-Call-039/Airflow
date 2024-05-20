from datetime import timedelta
from datetime import datetime
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from fsp.repeat_download import sql_query_to_csv
from commons.transfer_file_to_dbs import transfer_file_to_dbs
from indicators_to_regions.regions_editer import region_editer

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
    schedule_interval='50 6 * * *',
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
path_to_transfer_sql = f'{path_to_files}transfer/'

csv_request = 'request.csv'
csv_request_result = 'Requests .csv'
csv_transfer = 'Transfer текущий.csv'
csv_calls2 = 'calls 01_2023.csv'
csv_calls = 'calls текущий.csv'
csv_result = 'CallsTotal текущий.csv'
csv_result2 = 'CallsTotal архив 02_2024.csv'




dbs_result1 = '/Отчеты BI/Показатели до регионов/Заявки/'
dbs_result2 = '/Отчеты BI/Показатели до регионов/CallsTotal/'
dbs_result_transfer = '/Отчеты BI/Показатели до регионов/TransferRobot/'


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

# Преобразование файлов после sql.
region_editing = PythonOperator(
    task_id='region_editing', 
    python_callable=region_editer, 
    op_kwargs={'path_to_files': path_to_file_sql,'requests': csv_request,'calls': csv_calls, 'path_result': path_to_file,
                  'file_result_req': csv_request_result,'file_result': csv_result}, 
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

[request_sql, calls_sql, transfer_sql] >> region_editing >> [calls_to_dbs, request_to_dbs, transfer_to_dbs]

