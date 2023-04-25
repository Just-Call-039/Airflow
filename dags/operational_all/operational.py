import pendulum
import dateutil.relativedelta
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.transfer_files_to_dbs import transfer_files_to_dbs
from fsp.repeat_download import sql_query_to_csv

from operational_all.operational_editing import operational_transformation
from operational_all.operational_calls_editing import operational_calls_transformation



default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='operational',
    schedule_interval='*/90 8-20 * * *',
    start_date=pendulum.datetime(2023, 4, 24, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

# Пути к sql запросам на сервере airflow
path_to_sql_airflow = '/root/airflow/dags/operational_all/SQL/'
sql_operational = f'{path_to_sql_airflow}operational.sql'
sql_operational_calls = f'{path_to_sql_airflow}operational_calls.sql'
# sql_worktime = f'{path_to_sql_airflow}worktime.sql'
sql_etv = f'{path_to_sql_airflow}etv.sql'
sql_autofilling = f'{path_to_sql_airflow}autofilling.sql'


# Наименование файлов
file_name_users = 'users.csv'
file_name_operational = 'operational.csv'
file_name_operational_calls = 'operational_calls.csv'
# file_name_worktime = 'worktime.csv'
file_name_etv = 'etv.csv'
file_name_autofilling = 'autofilling.csv'

# Пути к файлам на сервере airflow
# Сразу после sql
path_to_file_airflow = '/root/airflow/dags/operational_all/Files/'
path_to_sql_operational_folder = f'{path_to_file_airflow}sql_operational/'

# Путь к пользователям
path_to_file_users = '/root/airflow/dags/fsp/Files/'

# После обработки питоном (итог)
path_to_operational_folder = f'{path_to_file_airflow}operational/'

# Пути к файлам на сервере dbs
path_to_file_dbs = '/operational/'
dbs_operational = f'{path_to_file_dbs}operational/'

# Выполнение SQL запросов
sql_operational = PythonOperator(
    task_id='operational_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_operational, 'path_csv_file': path_to_sql_operational_folder, 'name_csv_file': file_name_operational},
    dag=dag
    )

sql_operational_calls = PythonOperator(
    task_id='operational_calls_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_operational_calls, 'path_csv_file': path_to_sql_operational_folder, 'name_csv_file': file_name_operational_calls},
    dag=dag
    )

sql_etv = PythonOperator(
    task_id='etv_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_etv, 'path_csv_file': path_to_operational_folder, 'name_csv_file': file_name_etv},
    dag=dag
    )

sql_autofilling = PythonOperator(
    task_id='autofilling_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_autofilling, 'path_csv_file': path_to_operational_folder, 'name_csv_file': file_name_autofilling},
    dag=dag
    )


# Преобразование файлов после sql.
transformation_operational = PythonOperator(
    task_id='operational_transformation', 
    python_callable = operational_transformation, 
    op_kwargs={'path_to_users': path_to_file_users, 'name_users': file_name_users, 'path_to_folder': path_to_sql_operational_folder,
                'name_calls': file_name_operational, 'path_to_final_folder': path_to_operational_folder}, 
    dag=dag
    )

transformation_operational_calls = PythonOperator(
    task_id='operational_calls_transformation', 
    python_callable = operational_calls_transformation, 
    op_kwargs={'path_to_folder': path_to_sql_operational_folder, 'name_calls': file_name_operational, 'path_to_final_folder': path_to_operational_folder}, 
    dag=dag
    )


# Перенос всех файлов в папку DBS.
transfer_operational = PythonOperator(
    task_id='operational_transfer', 
    python_callable=transfer_files_to_dbs, 
    op_kwargs={'from_path': path_to_operational_folder, 'to_path': path_to_file_dbs, 'db': 'DBS'}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Ошибка выгрузки данных для оперативных отчетов',
        dag=dag,
        # on_failure_callback=True,
        # trigger_rule='all_success'
        trigger_rule='one_failed'
    )

# Очередности выполнения задач.

sql_operational >> transformation_operational
sql_operational_calls >> transformation_operational_calls
[transformation_operational, transformation_operational_calls, sql_etv, sql_autofilling] >> transfer_operational
# >> send_telegram_message


















