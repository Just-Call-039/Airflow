import datetime
import dateutil.relativedelta
from datetime import timedelta
import pendulum


from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

from fsp.repeat_download import sql_query_to_csv
from commons_sawa.telegram import telegram_send
from commons_li.clear_folder import clear_folder
from incoming_traffic.lids_edit import lids_editing


default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

def print_file_contents(file_path):
    with open(file_path, 'r') as file:
        file_contents = file.read()
        print(file_contents)

dag = DAG(
    dag_id='lids_and_waiters',
    schedule_interval='0 22 * * *',
    start_date=pendulum.datetime(2024, 1, 31, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

token = '5095756650:AAElXGJb5kfvanEXx5FlET6T3HayTjIs_PU'
chat_id = '-1001412983860'  # your chat id

today_date = datetime.now().strftime("%d/%m/%Y")
text_money = today_date +' Отправляем файл лидов и ждунов'


path_to_sql = '/root/airflow/dags/incoming_traffic/SQL/'
path_to_sql_lids = f'{path_to_sql}lids.sql'
path_to_sql_wait = f'{path_to_sql}waiters.sql'
path_to_sql_dop = f'{path_to_sql}dop.sql'



path_to_file_sql_airflow = '/root/airflow/dags/incoming_traffic/Files/sql_files/'
path_to_file_airflow = '/root/airflow/dags/incoming_traffic/Files/lids_sql/'

import datetime

today = datetime.date.today()
previous_date = today - datetime.timedelta(days=0)
year = previous_date.year
month = previous_date.month
day = previous_date.day

file_total = f'Лиды, обрывы, перезвоны {day}_{month}_{year}.xlsx' 
lids_csv = 'lids.csv'
wait_csv = 'wait.csv'
dop_csv = 'dop.csv'




# Блок выполнение sql запросов
lids_sql = PythonOperator(
    task_id='lids_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': path_to_sql_lids, 'path_csv_file': path_to_file_sql_airflow, 'name_csv_file': lids_csv}, 
    dag=dag
    )
wait_sql = PythonOperator(
    task_id='wait_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': path_to_sql_wait, 'path_csv_file': path_to_file_sql_airflow, 'name_csv_file': wait_csv}, 
    dag=dag
    )
dop_sql = PythonOperator(
    task_id='dop_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': path_to_sql_dop, 'path_csv_file': path_to_file_sql_airflow, 'name_csv_file': dop_csv}, 
    dag=dag
    )

# Блок обработки файла для отправки 
lids_wait_editing = PythonOperator(
    task_id='lids_wait_editing', 
    python_callable=lids_editing, 
    op_kwargs={'waiters': wait_csv, 'lids': lids_csv, 'path_to_file': path_to_file_sql_airflow, 'dop' : dop_csv,
               'file': file_total,'path_file': path_to_file_airflow}, 
    dag=dag
    )


send_telegram = PythonOperator(
    task_id='send_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_money, 'token': token, 'chat_id': chat_id, 'filepath': path_to_file_airflow, 'filename': file_total}, 
    dag=dag
    )

# Очистка папки

clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder, 
    op_kwargs={'folder': path_to_file_airflow}, 
    dag=dag
    )

[lids_sql, wait_sql, dop_sql] >> lids_wait_editing >> send_telegram >> clear_folders