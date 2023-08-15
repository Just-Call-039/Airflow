from datetime import timedelta
from datetime import datetime
# import datetime
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.repeat_download import sql_query_to_csv
from fsp.transfer_files_to_dbs import transfer_file_to_dbs
from incoming_line.inbound_calls_editer import inbound_editer

default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='incoming_line',
    schedule_interval='0 6 * * *',
    start_date=pendulum.datetime(2023, 7, 25, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'


path_to_sql = '/root/airflow/dags/incoming_line/SQL/'
sql_inbound = f'{path_to_sql}calls_inbound.sql'
sql_operator_calls = f'{path_to_sql}calls_operator.sql'
sql_request = f'{path_to_sql}request.sql'
sql_req_in = f'{path_to_sql}req_in.sql'
sql_user = f'{path_to_sql}Пользователи.sql'

path_to_files = '/root/airflow/dags/incoming_line/Files/'
path_to_file_sql = f'{path_to_files}sql_calls/'
path_to_file = f'{path_to_files}calls/'

csv_inbound = 'inbound.csv'
csv_operator_calls = 'operator_calls.csv'
csv_request = 'request.csv'
csv_req_in = 'req_in.csv'
csv_user = 'users.csv'
csv_result = 'Звонки входящей с заявками.csv'

dbs_result = '/Отчеты BI/Входящая линия/'

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

req_in_sql = PythonOperator(
    task_id='req_in_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_req_in, 'path_csv_file': path_to_file_sql, 'name_csv_file': csv_req_in}, 
    dag=dag
    )

users_sql = PythonOperator(
    task_id='users_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_user, 'path_csv_file': path_to_file_sql, 'name_csv_file': csv_user}, 
    dag=dag
    )

# Преобразование файлов после sql.
inbound_calls_editing = PythonOperator(
    task_id='inbound_calls_editing', 
    python_callable=inbound_editer, 
    op_kwargs={'path_to_files': path_to_file_sql, 'inbound': csv_inbound, 'operator_calls': csv_operator_calls, 'request': csv_request,'req_in': csv_req_in, 'path_result': path_to_file, 'file_result': csv_result}, 
    dag=dag
    )
    
transfer_inbound_calls = PythonOperator(
    task_id='transfer_inbound_calls', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file, 'to_path': dbs_result, 'db': 'DBS', 'file1': csv_result, 'file2': 'calls_phones.csv'}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Отчет входящей линии выгружен',
        dag=dag,
        # on_failure_callback=True,
        # trigger_rule='all_success'
    )
send_telegram_message_fiasko = TelegramOperator(
        task_id='send_telegram_error',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Ошибки выгрузки отчета входящей линии',
        dag=dag,
        # on_failure_callback=True,
        trigger_rule='one_failed'
    )




[inbound_sql,
operator_calls_sql,
request_sql,
req_in_sql,
users_sql] >> inbound_calls_editing >> transfer_inbound_calls >> [send_telegram_message,send_telegram_message_fiasko]
