from datetime import timedelta, date
import pendulum
import dateutil.relativedelta

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from commons.clear_file import clear_file
from commons.transfer_file_to_dbs import transfer_file_to_dbs
from commons.sql_query_to_csv import sql_query_to_csv


default_args = {
    'owner': 'Alexander Brezhnev',
    'email': 'brezhnev.aleksandr@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='request_for_steps_report',
    schedule_interval='30 6,12 * * *',
    start_date=pendulum.datetime(2023, 3, 13, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

today = date.today()
previous_date = today - dateutil.relativedelta.relativedelta(months=1)

year_for_previous = previous_date.year
month_for_previous = previous_date.month
file_name_previous = f'{year_for_previous}_{month_for_previous}.csv'

year_for_current = today.year
month_for_current = today.month
file_name_current = f'{year_for_current}_{month_for_current}.csv'

path_to_file_airflow = '/root/airflow/dags/steps_report/files/'
path_to_files_from_sql = f'{path_to_file_airflow}files_from_sql/'
path_to_main_folder = f'{path_to_file_airflow}main_folder/'
path_to_requests_folder = f'{path_to_file_airflow}requests_folder/'
path_to_uniqueid_medium_folder = f'{path_to_file_airflow}uniqueid_medium_folder/'

path_to_sql_airflow = '/root/airflow/dags/steps_report/SQL/'
sql_main = f'{path_to_sql_airflow}main.sql'
sql_requests_previous_month = f'{path_to_sql_airflow}requests_previous_month.sql'
sql_requests_current_month = f'{path_to_sql_airflow}requests_current_month.sql'

path_to_file_dbs = '/steps_report/files/'
dbs_from_sql = f'{path_to_file_dbs}files_from_sql/'
dbs_main_folder = f'{path_to_file_dbs}main_folder/'
dbs_requests_folder = f'{path_to_file_dbs}requests_folder/'
dbs_uniqueid_medium_folder = f'{path_to_file_dbs}uniqueid_medium_folder/'

# Блок выполнения SQL запросов.
requests_previous_month_sql = PythonOperator(
    task_id='requests_previous_month_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_requests_previous_month, 'path_csv_file': path_to_requests_folder, 'name_csv_file': file_name_previous}, 
    dag=dag
    )
requests_current_month_sql = PythonOperator(
    task_id='requests_current_month_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_requests_current_month, 'path_csv_file': path_to_requests_folder, 'name_csv_file': file_name_current}, 
    dag=dag
    )

[requests_previous_month_sql, requests_current_month_sql]
