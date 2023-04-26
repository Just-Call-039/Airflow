import pendulum
import dateutil.relativedelta
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.transfer_files_to_dbs import transfer_files_to_dbs
from fsp.transfer_files_to_dbs import remove_files_from_airflow
from fsp.repeat_download import repeat_download_data
from all_data.data_editing import data_transformation



default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='all_data',
    schedule_interval='30 15 * * 6',
    start_date=pendulum.datetime(2023, 4, 24, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'
x = 0
y = 10000000
count_repeats = 28
limit_list = [40000000,80000000,120000000,160000000,200000000,240000000]

# Пути к sql запросам на сервере airflow
path_to_sql_airflow = '/root/airflow/dags/all_data/SQL/'
sql_data = f'{path_to_sql_airflow}data.sql'


# Наименование файлов
file_data = 'Data_{}.csv'

# Пути к файлам на сервере airflow
# Сразу после sql
path_to_file_airflow = '/root/airflow/dags/all_data/Files/'
path_to_sql_data_folder = f'{path_to_file_airflow}sql_data/'

# После обработки питоном (итог)
path_to_data_folder = f'{path_to_file_airflow}data/'

# Пути к файлам на сервере dbs
path_to_file_dbs = '/scripts fsp/Current Files/Базы/'

# Выполнение SQL запросов
sql_data = PythonOperator(
    task_id='data_sql',
    python_callable=repeat_download_data,
    op_kwargs={'x': x, 'y': y, 'count_repeats': count_repeats, 'limit_list': limit_list, 'cloud': cloud_name, 'path_sql_file': sql_data, 'path_csv_file': path_to_sql_data_folder,
                'name_csv_file': file_data},
    dag=dag
    )

# Преобразование файлов после sql.
transformation_data = PythonOperator(
    task_id='data_transformation', 
    python_callable = data_transformation, 
    op_kwargs={'files_from_sql': path_to_sql_data_folder, 'files_to_csv': path_to_data_folder}, 
    dag=dag
    )

# Перенос всех файлов в папку DBS.
transfer_data = PythonOperator(
    task_id='data_transfer', 
    python_callable=transfer_files_to_dbs, 
    op_kwargs={'from_path': path_to_data_folder, 'to_path': path_to_file_dbs, 'db': 'DBS'}, 
    dag=dag
    )

# remove_files_from_airflow = PythonOperator(
#     task_id='remove_sql_files_from_airflow', 
#     python_callable=remove_files_from_airflow, 
#     op_kwargs={'paths': [path_to_sql_data_folder, path_to_data_folder]}, 
#     dag=dag
#     )

# # Отправка уведомления об ошибке в Telegram.
# send_telegram_message = TelegramOperator(
#         task_id='send_telegram_message',
#         telegram_conn_id='Telegram',
#         chat_id='-1001412983860',
#         text='Ошибка выгрузки данных для отчета Базы 2.0',
#         dag=dag,
#         # on_failure_callback=True,
#         # trigger_rule='all_success'
#         trigger_rule='one_failed'
#     )

# Очередности выполнения задач.
sql_data >> transformation_data >> transfer_data 
# >> remove_files_from_airflow >> send_telegram_message

















