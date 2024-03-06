from datetime import timedelta
from datetime import datetime
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from fsp.repeat_download import sql_query_to_csv
from fsp.transfer_files_to_dbs import transfer_file_to_dbs
from request_with_calls_today.request_with_calls_editer import request_editer

default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='request_today',
    schedule_interval='*/20 6-19 * * *',
    start_date=pendulum.datetime(2023, 7, 7, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args,
    max_active_runs=1 
    )


cloud_name = 'cloud_128'

path_to_sql = '/root/airflow/dags/request_with_calls_today/SQL/'
sql_request = f'{path_to_sql}Request.sql'
sql_user = '/root/airflow/dags/incoming_line/SQL/Пользователи.sql'


path_to_files = '/root/airflow/dags/request_with_calls_today/Files/'
path_to_file_sql = f'{path_to_files}sql_total/'
path_to_file = f'{path_to_files}request/'

csv_request = 'request.csv'
csv_result = 'Заявки.csv'
csv_user = 'users.csv'



dbs_result = '/4_report/new files/'


# Блок выполнения SQL запросов.


request_sql = PythonOperator(
    task_id='request_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_request, 'path_csv_file': path_to_file_sql, 'name_csv_file': csv_request}, 
    dag=dag
    )
users_sql = PythonOperator(
    task_id='users_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_user, 'path_csv_file': path_to_files, 'name_csv_file': csv_user}, 
    dag=dag
    )

# Преобразование файлов после sql.
request_editing = PythonOperator(
    task_id='request_editing', 
    python_callable=request_editer, 
    op_kwargs={'path_to_files': path_to_file_sql, 'request': csv_request, 'path_result': path_to_file, 'file_result': csv_result}, 
    dag=dag
    )

requst_to_dbs = PythonOperator(
    task_id='requst_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file, 'to_path': dbs_result, 'db': 'DBS', 'file1': csv_result, 'file2': ''}, 
    dag=dag
    )

request_sql >> request_editing >> requst_to_dbs