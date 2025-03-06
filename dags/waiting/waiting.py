from datetime import timedelta, date
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from commons.sql_query_to_csv import sql_query_to_csv
from waiting.waiting_to_click import to_click
from waiting.datalens_csv import union_csv


default_args = {
    'owner': 'Alexander Brezhnev',
    'email': 'brezhnev.aleksandr@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='waiting',
    schedule_interval='30 21 * * *',
    start_date=pendulum.datetime(2023, 4, 11, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'
# cloud_name = 'cloud_183'

# Пути к sql запросам на сервере airflow.
sql = '/root/airflow/dags/waiting/SQL/waiting_log.sql'
sql_datalens = '/root/airflow/dags/waiting/SQL/waiting_datalens.sql'


# Пути к файлам на сервере airflow.
path_to_file_airflow = '/root/airflow/dags/waiting/files/'

# Текущая дата.
now = date.today() 
# Наименование файла для ждунов за текущий день.
start = '2024-07-01'
end = '2024-11-06'
file_name = f'Ждуны за {now}.csv'
file_name_datalens = 'Waiters.csv'
# Пути к файлам на сервере dbs.
path_to_file_dbs = '/Shoooorik/Waiters/Waiters/'

# Блок выполнения SQL запросов.
waiting_sql = PythonOperator(
    task_id='waiting_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql, 'path_csv_file': path_to_file_airflow, 'name_csv_file': file_name, 'current_separator': ','}, 
    dag=dag
    )

# Блок выполнения SQL запросов.
waiting_datalens = PythonOperator(
    task_id='waiting_datalens', 
    python_callable=union_csv, 
    op_kwargs={'path_to_folder': path_to_file_airflow, 'start': start, 'end' : end, 'final_name': file_name_datalens}, 
    dag=dag
    )

# Блок отправки всех файлов в папку DBS.
waiting_sql_to_dbs = PythonOperator(
    task_id='waiting_sql_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': file_name, 'db': 'DBS'}, 
    dag=dag
    )

# Блок отправки всех файлов в ClickHouse
waiting_sql_to_ch = PythonOperator(
    task_id='waiting_sql_to_ch', 
    python_callable=to_click, 
    op_kwargs={'path_to_file': path_to_file_airflow, 'file_name': file_name}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Произошла ошибка в работе ждунов.',
        dag=dag,
        # on_failure_callback=True,
        # trigger_rule='all_success'
        trigger_rule='one_failed'
    )

# Блок очередности выполнения задач.
waiting_sql >> waiting_sql_to_dbs >> send_telegram_message
waiting_sql >> waiting_sql_to_ch >> send_telegram_message
waiting_datalens
