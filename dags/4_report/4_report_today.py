from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

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
    dag_id='4_report_today',
    schedule_interval='30 7-14 * * *',
    start_date=pendulum.datetime(2022, 11, 22, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

# Наименование файлов.
csv_main = 'main.csv'
csv_requests = 'requests.csv'

# Пути к sql запросам на сервере airflow.
path_to_sql_airflow = '/root/airflow/dags/4_report/SQL/'
sql_main = f'{path_to_sql_airflow}main_current_day.sql'
sql_requests = f'{path_to_sql_airflow}requests_current_day.sql'

# Пути к файлам на сервере airflow.
path_to_file_airflow = '/root/airflow/dags/4_report/Files/today/'
path_to_main_today = f'{path_to_file_airflow}main_folder/'
path_to_requests_today = f'{path_to_file_airflow}requests_folder/'

# Пути к файлам на сервере dbs.
path_to_file_dbs = '/4_report/Files/today/'
dbs_main_today = f'{path_to_file_dbs}main_folder/'
dbs_requests_today = f'{path_to_file_dbs}requests_folder/'

# Блок выполнения SQL запросов.
requests_today = PythonOperator(
    task_id='requests_today', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_requests, 'path_csv_file': path_to_requests_today, 'name_csv_file': csv_requests}, 
    dag=dag
    )
main_today = PythonOperator(
    task_id='main_today', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main, 'path_csv_file': path_to_main_today, 'name_csv_file': csv_main}, 
    dag=dag
    )

# Блок отправки всех файлов в папку DBS.
requests_today_to_dbs = PythonOperator(
    task_id='requests_today_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_requests_today, 'to_path': dbs_requests_today, 'file': csv_requests, 'db': 'DBS'}, 
    dag=dag
    )
main_today_to_dbs = PythonOperator(
    task_id='main_today_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_main_today, 'to_path': dbs_main_today, 'file': csv_main, 'db': 'DBS'}, 
    dag=dag
    )

requests_today >> requests_today_to_dbs
main_today >> main_today_to_dbs
