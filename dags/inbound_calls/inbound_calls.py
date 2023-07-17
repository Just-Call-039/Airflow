from datetime import timedelta
from datetime import datetime
# import datetime
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.repeat_download import sql_query_to_csv
from fsp.transfer_files_to_dbs import transfer_file_to_dbs
from inbound_calls.inbound_calls_editer import inbound_editer

default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='inbound_calls',
    schedule_interval='50 6 * * *',
    start_date=pendulum.datetime(2022, 11, 22, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'


path_to_sql = '/root/airflow/dags/inbound_calls/SQL/'
sql_inbound = f'{path_to_sql}inbound.sql'
sql_operator_calls = f'{path_to_sql}operator_calls.sql'
sql_request = f'{path_to_sql}request.sql'

path_to_files = '/root/airflow/dags/inbound_calls/Files/'
path_to_file_sql = f'{path_to_files}sql_calls/'
path_to_file = f'{path_to_files}calls/'


csv_inbound = 'inbound.csv'
csv_operator_calls = 'operator_calls.csv'
csv_request = 'request.csv'
csv_result = 'Входящая линия.csv'

dbs_result = '/scripts fsp/Current Files/'

# Блок выполнения SQL запросов.
inbound_sql = PythonOperator(
    task_id='inbound_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_inbound, 'path_csv_file': path_to_file_sql, 'name_csv_file': csv_inbound}, 
    dag=dag
    )

operator_calls_sql = PythonOperator(
    task_id='operator_calls_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_operator_calls, 'path_csv_file': path_to_file_sql, 'name_csv_file': csv_operator_calls}, 
    dag=dag
    )

request_sql = PythonOperator(
    task_id='request_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_request, 'path_csv_file': path_to_file_sql, 'name_csv_file': csv_request}, 
    dag=dag
    )

# Преобразование файлов после sql.
inbound_calls_editing = PythonOperator(
    task_id='inbound_calls_editing', 
    python_callable=inbound_editer, 
    op_kwargs={'path_to_files': path_to_file_sql, 'inbound': csv_inbound, 'operator_calls': csv_operator_calls, 'request': csv_request, 'path_result': path_to_file, 'file_result': csv_result}, 
    dag=dag
    )
    
transfer_inbound_calls = PythonOperator(
    task_id='transfer_inbound_calls', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file, 'to_path': dbs_result, 'db': 'DBS', 'file1': csv_result, 'file2': ''}, 
    dag=dag
    )


[inbound_sql,
operator_calls_sql,
request_sql] >> inbound_calls_editing >> transfer_inbound_calls
