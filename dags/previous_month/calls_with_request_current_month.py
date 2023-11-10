import datetime
import dateutil.relativedelta
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv

default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='calls_with_request_current',
    schedule_interval='50 5-17 * * *',
    start_date=pendulum.datetime(2023, 7, 7, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args,
    max_active_runs=1 
    )


cloud_name = 'cloud_128'

path_to_file_airflow = '/root/airflow/dags/previous_month/Files/'
path_airflow_calls_with_request = f'{path_to_file_airflow}calls_with_request/'

path_to_file_dbs = '/4_report/new files/'
path_dbs_calls_with_request = f'{path_to_file_dbs}Звонки для заявок/'

sql_main = '/root/airflow/dags/previous_month/SQL/Звонки для заявок.sql'

csv_calls = 'Звонки для заявок текущий.csv'  
# Блок выполнения SQL запросов.
calls_with_request_sql = PythonOperator(
    task_id='calls_with_request_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main, 'path_csv_file': path_airflow_calls_with_request, 'name_csv_file': csv_calls}, 
    dag=dag
    )

# Блок отправки всех файлов в папку DBS.
calls_with_request_to_dbs = PythonOperator(
    task_id='calls_with_request_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_calls_with_request, 'to_path': path_dbs_calls_with_request, 'file': csv_calls, 'db': 'DBS'}, 
    dag=dag
    )


# Блок очередности выполнения задач.
calls_with_request_sql >> calls_with_request_to_dbs 

