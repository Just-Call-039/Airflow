import pendulum
import datetime
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
    'mysql_conn_id': 'cloud_my_sql_117',
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5)
    }

dag = DAG(
    dag_id='10_report_previous_month',
    schedule_interval='50 5-13/4 * * *',
    start_date=pendulum.datetime(2022, 6, 16, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


path_to_file_airflow = '/root/airflow/dags/10_report/Files/'
path_airflow_main_folder = f'{path_to_file_airflow}main_folder/'
path_airflow_transfer_folder = f'{path_to_file_airflow}transfer_folder/'

path_to_file_dbs = '/10_report/Files/'
path_dbs_main_folder = f'{path_to_file_dbs}main_folder/'
path_dbs_transfer_folder = f'{path_to_file_dbs}transfer_folder/'

cloud_name = '72'
sql_folder = '/root/airflow/dags/10_report/SQL/'
sql_main = f'{sql_folder}main_previous_month.sql'
sql_transfer = f'{sql_folder}transfer_previous_month.sql'

today = datetime.date.today()
previous_date = today - dateutil.relativedelta.relativedelta(months=1)
year = previous_date.year
month = previous_date.month
file_name = f'{year}_{month}.csv'

# Блок выполнения SQL запросов.
main_folder_sql = PythonOperator(
    task_id='main_folder_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main, 'path_csv_file': path_airflow_main_folder, 'name_csv_file': file_name}, 
    dag=dag
    )
transfer_folder_sql = PythonOperator(
    task_id='transfer_folder_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_transfer, 'path_csv_file': path_airflow_transfer_folder, 'name_csv_file': file_name}, 
    dag=dag
    )

# Блок отправки всех файлов в папку DBS.
main_transfer_to_dbs = PythonOperator(
    task_id='main_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_main_folder, 'to_path': path_dbs_main_folder, 'file': 'main_current_month.csv', 'db': 'DBS'}, 
    dag=dag
    )
transfer_folder_transfer_to_dbs = PythonOperator(
    task_id='transfer_folder_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_transfer_folder, 'to_path': path_dbs_transfer_folder, 'file': 'transfer_current_month.csv', 'db': 'DBS'}, 
    dag=dag
    )

main_folder_sql >> main_transfer_to_dbs
transfer_folder_sql >> transfer_folder_transfer_to_dbs
