from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from commons.sql_query_to_csv import sql_query_to_csv
from report_25.status_dict import status_dict


default_args = {
    'owner': 'Alexander Brezhnev',
    'email': 'brezhnev.aleksandr@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'mysql_conn_id': 'cloud_my_sql_117',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    }

dag = DAG(
    dag_id='25_report',
    schedule_interval='15 6 * * *',
    start_date=pendulum.datetime(2022, 11, 22, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

path_to_file_airflow = '/root/airflow/dags/report_25/Files/'
path_to_file_dbs = '/25_report/Files/'
path_to_sql = '/root/airflow/dags/report_25/SQL/'

sql_status = f'{path_to_sql}Status.sql'
sql_my_request_yesterday_window = f'{path_to_sql}My_request_yesterday_window.sql'
sql_total_calls_yesterday = f'{path_to_sql}Total_calls_yesterday.sql'

# Блок выполнения SQL запросов.
status_sql = PythonOperator(
    task_id='status_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_status, 'path_csv_file': path_to_file_airflow, 'name_csv_file': 'status.csv'}, 
    dag=dag
    )
my_request_yesterday_window_sql = PythonOperator(
    task_id='my_request_yesterday_window_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_my_request_yesterday_window, 'path_csv_file': path_to_file_airflow, 'name_csv_file': 'my_request_yesterday_window.csv'}, 
    dag=dag
    )
total_calls_yesterday_sql = PythonOperator(
    task_id='total_calls_yesterday_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_total_calls_yesterday, 'path_csv_file': path_to_file_airflow, 'name_csv_file': 'total_calls_yesterday.csv'}, 
    dag=dag
    )

# Блок отправки файлов в папку DBS.
status_transfer_to_dbs = PythonOperator(
    task_id='status_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'status.csv', 'db': 'DBS'}, 
    dag=dag
    )
my_request_yesterday_window_transfer_to_dbs = PythonOperator(
    task_id='my_request_yesterday_window_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'my_request_yesterday_window.csv', 'db': 'DBS'}, 
    dag=dag
    )
total_calls_yesterday_transfer_to_dbs = PythonOperator(
    task_id='total_calls_yesterday_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'total_calls_yesterday.csv', 'db': 'DBS'}, 
    dag=dag
    )

# Создание словаря со статусами, запись словаря в файл, перенос на DBS.
status_dict_to_file = PythonOperator(
    task_id='status_dict_to_file',
    python_callable=status_dict,
    op_kwargs={'status': f'{path_to_file_airflow}status.csv', 'to_file_status_dict': f'{path_to_file_airflow}status_dict.csv'}
)
status_dict_transfer_to_dbs = PythonOperator(
    task_id='status_dict_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'status_dict.csv', 'db': 'DBS'}, 
    dag=dag
    )

status_sql >> [status_transfer_to_dbs, status_dict_to_file] >> status_dict_transfer_to_dbs
my_request_yesterday_window_sql >> my_request_yesterday_window_transfer_to_dbs
total_calls_yesterday_sql >> total_calls_yesterday_transfer_to_dbs
