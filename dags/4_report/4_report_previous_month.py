from datetime import timedelta
import pendulum
import datetime
import dateutil.relativedelta

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
    'mysql_conn_id': 'cloud_my_sql_117',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.datetime(2022, 11, 22, tz='Europe/Kaliningrad'),
    'catchup': False
    }

dag = DAG(
    dag_id='4_report_previous_month',
    schedule_interval='00 6 1 * 1,4',
    default_args=default_args
    )


path_to_file_airflow = '/root/airflow/dags/4_report/Files/'
path_airflow_main_folder = f'{path_to_file_airflow}main_folder/'
path_airflow_requests_folder = f'{path_to_file_airflow}requests_folder/'
path_airflow_working_time_folder = f'{path_to_file_airflow}working_time_folder/'

path_to_file_dbs = '/4_report/Files/'
path_dbs_main_folder = f'{path_to_file_dbs}main_folder/'
path_dbs_requests_folder = f'{path_to_file_dbs}requests_folder/'
path_dbs_working_time_folder = f'{path_to_file_dbs}working_time_folder/'

cloud_name = 'cloud_117'
sql_main = '/root/airflow/dags/4_report/SQL/main_previous_month.sql'
sql_requests = '/root/airflow/dags/4_report/SQL/requests_previous_month.sql'
sql_working_time = '/root/airflow/dags/4_report/SQL/working_time_previous_month.sql'

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
requests_folder_sql = PythonOperator(
    task_id='requests_folder_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_requests, 'path_csv_file': path_airflow_requests_folder, 'name_csv_file': file_name}, 
    dag=dag
    )
working_time_folder_sql = PythonOperator(
    task_id='working_time_folder_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_working_time, 'path_csv_file': path_airflow_working_time_folder, 'name_csv_file': file_name}, 
    dag=dag
    )

# Блок отправки всех файлов в папку DBS.
main_transfer_to_dbs = PythonOperator(
    task_id='main_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_main_folder, 'to_path': path_dbs_main_folder, 'file': file_name, 'db': 'DBS'}, 
    dag=dag
    )
requests_transfer_to_dbs = PythonOperator(
    task_id='requests_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_requests_folder, 'to_path': path_dbs_requests_folder, 'file': file_name, 'db': 'DBS'}, 
    dag=dag
    )
working_transfer_to_dbs = PythonOperator(
    task_id='working_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_working_time_folder, 'to_path': path_dbs_working_time_folder, 'file': file_name, 'db': 'DBS'}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Произошла ошибка работы отчета №4 предыдущего месяца.',
        dag=dag,
        # on_failure_callback=True,
        # trigger_rule='all_success'
        trigger_rule='one_failed'
    )

# Блок очередности выполнения задач.
main_folder_sql >> main_transfer_to_dbs >> send_telegram_message
requests_folder_sql >> requests_transfer_to_dbs >> send_telegram_message
working_time_folder_sql >> working_transfer_to_dbs >> send_telegram_message
