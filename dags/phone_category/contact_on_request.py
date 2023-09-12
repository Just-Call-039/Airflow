from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from phone_category.contact_to_clickhouse import contact_editer
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
    dag_id='phone_category',
    schedule_interval='0 6 * * *',
    start_date=pendulum.datetime(2023, 7, 7, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

# Наименование файлов.
csv_req = 'request.csv' 

# Пути к sql запросам на сервере airflow.
path_to_sql_airflow = '/root/airflow/dags/phone_category/SQL/'
sql_req = f'{path_to_sql_airflow}req.sql'

# Пути к файлам на сервере airflow.
path_to_file_airflow = '/root/airflow/dags/phone_category/Files/'

# Блок выполнения SQL запросов.

request_load = PythonOperator(
    task_id='request_load', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_req, 'path_csv_file': path_to_file_airflow, 'name_csv_file': csv_req}, 
    dag=dag
    )

# Блок загрузки данный в ClickHouse 

phone_request_to_clickhouse = PythonOperator(
    task_id='phone_request_to_clickhouse', 
    python_callable=contact_editer, 
    op_kwargs={'path_to_files': path_to_file_airflow, 'requests': csv_req}, 
    dag=dag
    )
request_load >> phone_request_to_clickhouse