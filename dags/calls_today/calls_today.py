from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv

default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='calls_today',
    schedule_interval='0 6-18 * * *',
    start_date=pendulum.datetime(2023, 7, 7, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

# Наименование файлов.
csv_calls = 'Звонки сегодня.csv' 

# Пути к sql запросам на сервере airflow.
path_to_sql_airflow = '/root/airflow/dags/calls_today/SQL/'
sql_calls = f'{path_to_sql_airflow}Звонки сегодня.sql'

# Пути к файлам на сервере airflow.
path_to_file_airflow = '/root/airflow/dags/calls_today/Files/'

# Пути к файлам на сервере dbs.
path_to_file_dbs = '/4_report/new files/calls/'

# Блок выполнения SQL запросов.

calls_today = PythonOperator(
    task_id='calls_today', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_calls, 'path_csv_file': path_to_file_airflow, 'name_csv_file': csv_calls}, 
    dag=dag
    )

# Блок отправки всех файлов в папку DBS.

calls_today_to_dbs = PythonOperator(
    task_id='calls_today_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': csv_calls, 'db': 'DBS'}, 
    dag=dag
    )

calls_today >> calls_today_to_dbs 




























