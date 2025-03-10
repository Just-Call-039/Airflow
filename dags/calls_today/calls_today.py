from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv
from calls_today.transfer_in_clickhouse import to_click
from calls_today.transfer_in_clickhouse_v2 import call_to_click

from calls_today.transfer_in_clickhouse_v2 import call_10_to_click



default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    dag_id='calls_today',
    schedule_interval='0 6-18 * * *',
    start_date=pendulum.datetime(2023, 7, 7, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args,
    max_active_runs=1
    )


cloud_name = 'cloud_128'

# Наименование файлов.
csv_calls = 'Звонки_сегодня.csv' 
csv_calls_4 = 'Звонки_4_report.csv'

# Пути к sql запросам на сервере airflow.
path_to_sql_airflow = '/root/airflow/dags/calls_today/SQL/'
sql_calls = f'{path_to_sql_airflow}Звонки сегодня.sql'
sql_calls_4= f'{path_to_sql_airflow}Звонки for 4 report.sql'
sql_calls_10 = f'{path_to_sql_airflow}calls_now.sql'

# Пути к файлам на сервере airflow.
path_to_file_airflow = '/root/airflow/dags/calls_today/Files/'
path_to_file_airflow_10 = '/root/airflow/dags/calls_today/Files/10/'

# Пути к файлам на сервере dbs.
path_to_file_dbs = '/4_report/new files/calls/'
path_to_file_dbs_10 = '/10_otchet_partners/Calls/'

# Блок выполнения SQL запросов.

calls_today = PythonOperator(
    task_id='calls_today', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_calls, 'path_csv_file': path_to_file_airflow, 'name_csv_file': csv_calls}, 
    dag=dag
    )

calls_today_4 = PythonOperator(
    task_id='calls_today_4', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_calls_4, 'path_csv_file': path_to_file_airflow, 'name_csv_file': csv_calls_4}, 
    dag=dag
    )

calls_today_10 = PythonOperator(
    task_id='calls_today_10', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_calls_10, 'path_csv_file': path_to_file_airflow_10, 'name_csv_file': csv_calls}, 
    dag=dag
    )

# Блок отправки всех файлов в папку DBS.

calls_today_to_dbs = PythonOperator(
    task_id='calls_today_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': csv_calls, 'db': 'DBS'}, 
    dag=dag
    )
calls_today_10_to_dbs = PythonOperator(
    task_id='calls_today_10_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow_10, 'to_path': path_to_file_dbs_10, 'file': csv_calls, 'db': 'DBS'}, 
    dag=dag
    )


transfer_calls_to_click = PythonOperator(
    task_id='transfer_calls_to_click', 
    python_callable=call_to_click, 
    op_kwargs={'path_file': path_to_file_airflow, 'call': csv_calls_4}, 
    dag=dag
    )

transfer_call_10_to_click = PythonOperator(
    task_id='transfer_call_10_to_click', 
    python_callable=call_10_to_click, 
    op_kwargs={'path_file': path_to_file_airflow_10, 'call_10': csv_calls}, 
    dag=dag
    )

calls_today >> calls_today_to_dbs
calls_today_4 >> transfer_calls_to_click
calls_today_10 >> [calls_today_10_to_dbs, transfer_call_10_to_click]





























