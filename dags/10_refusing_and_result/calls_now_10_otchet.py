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
    dag_id='calls_10_otchet_now',
    schedule_interval='50 5-17 * * *',
    start_date=pendulum.datetime(2023, 7, 11, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

# Наименование файлов.
csv_calls = 'Звонки сегодня.csv' 

# Пути к sql запросам на сервере airflow.
path_to_sql_airflow = '/root/airflow/dags/10_refusing_and_result/SQL/'
sql_calls = f'{path_to_sql_airflow}calls_now.sql'

# Пути к файлам на сервере airflow.
path_to_file_airflow = '/root/airflow/dags/10_refusing_and_result/Files/'

# Пути к файлам на сервере dbs.
path_to_file_dbs = '/10_otchet_partners/Calls/'

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




























