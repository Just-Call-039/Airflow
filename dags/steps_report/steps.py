from datetime import timedelta, date
import pendulum

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
    dag_id='steps_report',
    schedule_interval='30 6,10,14 * * *',
    start_date=pendulum.datetime(2023, 3, 14, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

today = date.today()
year = today.strftime('%Y')
month = today.strftime('%m')
day = today.strftime('%d')

file_name = f'{year}_{month}_{day}.csv'

path_to_file_airflow = '/root/airflow/dags/steps_report/files/'
path_to_files_from_sql = f'{path_to_file_airflow}files_from_sql/'
path_to_main_folder = f'{path_to_file_airflow}main_folder/'


path_to_sql_airflow = '/root/airflow/dags/steps_report/SQL/'

path_to_file_dbs = '/steps_report/files/'

sql_main = f'{path_to_sql_airflow}main.sql'
sql_request = f'{path_to_sql_airflow}request_only_one_day.sql'
