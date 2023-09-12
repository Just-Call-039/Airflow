from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv

default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    dag_id='beeline_lids',
    schedule_interval='0 6 * * FRI',
    start_date=pendulum.datetime(2023, 9,12, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

# Наименование файлов.
csv_calls = 'Звонки 9_2023.csv' 
csv_work = 'work_time.csv'

# Пути к sql запросам на сервере airflow.
path_to_sql_airflow = '/root/airflow/dags/beeline_lids/SQL/'
sql_calls = f'{path_to_sql_airflow}calls_by_month.sql'
sql_work = f'{path_to_sql_airflow}work_login.sql'

# Пути к файлам на сервере airflow.
path_to_file_airflow = '/root/airflow/dags/beeline_lids/Files/'

# Пути к файлам на сервере dbs.
work_to_file_dbs = '/4_report/beeline/work/'
calls_to_file_dbs = '/4_report/beeline/calls/'

# Блок выполнения SQL запросов.

calls_beeline= PythonOperator(
    task_id='calls_beeline', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_calls, 'path_csv_file': path_to_file_airflow, 'name_csv_file': csv_calls}, 
    dag=dag
    )
work_time = PythonOperator(
    task_id='work_time', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_work, 'path_csv_file': path_to_file_airflow, 'name_csv_file': csv_work}, 
    dag=dag
    )

# Блок отправки всех файлов в папку DBS.

calls_beeline_to_dbs = PythonOperator(
    task_id='calls_beeline_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': calls_to_file_dbs, 'file': csv_calls, 'db': 'DBS'}, 
    dag=dag
    )
work_time_to_dbs = PythonOperator(
    task_id='work_time_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': work_to_file_dbs, 'file': csv_work, 'db': 'DBS'}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Подробный отчет по Билайну выгружен',
        dag=dag,
        # on_failure_callback=True,
        # trigger_rule='all_success'
    )
send_telegram_message_fiasko = TelegramOperator(
        task_id='send_telegram_error',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Ошибка выгрузки подробного отчета по Билайну',
        dag=dag,
        # on_failure_callback=True,
        trigger_rule='one_failed'
    )

calls_beeline >> calls_beeline_to_dbs >> [send_telegram_message, send_telegram_message_fiasko]
work_time >> work_time_to_dbs >> [send_telegram_message, send_telegram_message_fiasko]




 























