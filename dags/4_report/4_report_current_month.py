from datetime import timedelta
import pendulum
import datetime
import dateutil.relativedelta

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from commons.transfer_file import transfer_file
from commons.transfer_file_to_dbs import transfer_file_to_dbs
from commons.del_file import del_file


default_args = {
    'owner': 'Alexander Brezhnev',
    'email': 'brezhnev.aleksandr@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'mysql_conn_id': 'cloud_my_sql_117',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.datetime(2022, 11, 22, tz='Europe/Kaliningrad'),
    'catchup': False
    }

dag = DAG(
    dag_id='4_report_current_month',
    schedule_interval='0 9-15/2 * * *',
    default_args=default_args
    )


path_to_file_airflow = '/root/airflow/dags/4_report/Files/'
path_airflow_main_folder = f'{path_to_file_airflow}main_folder/'
path_airflow_requests_folder = f'{path_to_file_airflow}requests_folder/'
path_airflow_working_time_folder = f'{path_to_file_airflow}working_time_folder/'

path_to_file_mysql = '/home/glotov/192.168.1.117/4_report/'
path_mysql_main_folder = f'{path_to_file_mysql}main_folder/'
path_mysql_requests_folder = f'{path_to_file_mysql}requests_folder/'
path_mysql_working_time_folder = f'{path_to_file_mysql}working_time_folder/'

path_to_file_dbs = '/4_report/Files/'
path_dbs_main_folder = f'{path_to_file_dbs}main_folder/'
path_dbs_requests_folder = f'{path_to_file_dbs}requests_folder/'
path_dbs_working_time_folder = f'{path_to_file_dbs}working_time_folder/'

today = datetime.date.today()
previous_date = today - dateutil.relativedelta.relativedelta(months=1)
year = previous_date.year
month = previous_date.month
file_name = f'{year}_{month}.csv'

# Блок выполнения SQL запросов.
main_sql = MySqlOperator(
    task_id='main_sql', 
    sql='/SQL/Main_to_csv.sql', 
    dag=dag
    )
working_time_sql = MySqlOperator(
    task_id='working_time_sql', 
    sql='/SQL/Working_time_to_csv.sql', 
    dag=dag
    )
users_total_sql = MySqlOperator(
    task_id='users_total_sql', 
    sql='/SQL/Users_total_to_csv.sql', 
    dag=dag
    )
