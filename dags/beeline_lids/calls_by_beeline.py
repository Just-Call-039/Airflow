from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv
from beeline_lids.transfer_to_click_beeline import beeline_clickhouse

default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='beeline_lids',
    schedule_interval='0 6 * * *',
    start_date=pendulum.datetime(2023, 9,12, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


# cloud_name = 'cloud_128'
cloud_name = 'cloud_183'

# Наименование файлов.
csv_calls = 'calls.csv'
csv_work = 'work_time.csv'
csv_otkaz = 'decoding_new.csv'


# Пути к sql запросам на сервере airflow.
path_to_sql_airflow = '/root/airflow/dags/beeline_lids/SQL/'
sql_calls = f'{path_to_sql_airflow}calls_by_month.sql'
sql_work = f'{path_to_sql_airflow}work_login.sql'

# Пути к файлам на сервере airflow.
path_to_file_airflow = '/root/airflow/dags/beeline_lids/Files/'

# Блок выполнения SQL запросов.

calls_beeline= PythonOperator(
    task_id='calls_beeline', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_calls, 'path_csv_file': path_to_file_airflow, 'name_csv_file': csv_calls}, 
    dag=dag
    )
work_time = PythonOperator(
    task_id='work_time', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_work, 'path_csv_file': path_to_file_airflow, 'name_csv_file': csv_work}, 
    dag=dag
    )

beeline_to_clickhouse = PythonOperator(
    task_id='beeline_to_clickhouse', 
    python_callable=beeline_clickhouse,
    op_kwargs={'path_to_files': path_to_file_airflow, 'calls': csv_calls, 'work': csv_work, 'otkaz': csv_otkaz}, 
    dag=dag
    )


[calls_beeline,work_time] >> beeline_to_clickhouse




 























