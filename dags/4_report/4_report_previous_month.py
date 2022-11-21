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
    'mysql_conn_id': 'Maria_db',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.datetime(2022, 8, 7, tz='Europe/Kaliningrad'),
    'catchup': False
    }

dag = DAG(
    dag_id='4_report_previous_month',
    schedule_interval='0 9-15/2 * * *',
    default_args=default_args
    )


path_to_file_airflow = '/root/airflow/dags/4_report/Files/'
path_airflow_main_folder = '/root/airflow/dags/4_report/Files/main_folder/'
path_airflow_requests_folder = '/root/airflow/dags/4_report/Files/requests_folder/'
path_airflow_working_time_folder = '/root/airflow/dags/4_report/Files/working_time_folder/'

path_to_file_mysql = '/home/glotov/192.168.1.117/4_report/'
path_mysql_main_folder = '/home/glotov/192.168.1.117/4_report/main_folder/'
path_mysql_requests_folder = '/home/glotov/192.168.1.117/4_report/requests_folder/'
path_mysql_working_time_folder = '/home/glotov/192.168.1.117/4_report/working_time_folder/'

path_to_file_dbs = '/4_report/Files/'
path_dbs_main_folder = '/4_report/Files/main_folder/'
path_dbs_requests_folder = '/4_report/Files/requests_folder/'
path_dbs_working_time_folder = '/4_report/Files/working_time_folder/'
