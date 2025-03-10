import pendulum
import dateutil.relativedelta
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable


from commons.transfer_file_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv
from fsp.transfer_files_to_dbs import transfer_files_to_dbs
from commons_li.clear_folder import clear_folder


from operational_all.operational_accumulate_dozvon import transfer_files_to_click, transfer_dozvon_to_dbs
from operational_all.operational_accumulate_update import update_operational
from operational_all.operational_etv import etv_universal





default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='operational_accumulate',
    schedule_interval='0 1 * * *',
    start_date=pendulum.datetime(2023, 7, 25, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )



# cloud_name = 'cloud_128'
cloud_name = 'cloud_183'

n = 2
stop = 14






# Пути к sql запросам на сервере airflow
path_to_sql_airflow = '/root/airflow/dags/operational_all/SQL/'
sql_operational = f'{path_to_sql_airflow}operational_accumulate.sql'
sql_etv = f'{path_to_sql_airflow}etv.sql'
sql_autofilling = f'{path_to_sql_airflow}autofilling.sql'


# Наименование файлов
file_name_users = 'users.csv'


today = datetime.date.today()
year = today.year
month = today.month
day = today.day
file_name_autofilling = f'Автозаливки_{month:02}_{day:02}.csv' 
file_name_operational = 'Оперативный_архив_{}.csv'
file_name_etv = 'ЕТВ.csv'

file_name_dozvon = 'Дозвон.csv'
file_name_dozvon_per_day = f'Дозвон_{month:02}_{day:02}.csv'


# Пути к файлам на сервере airflow
# Сразу после sql
path_to_file_airflow = '/root/airflow/dags/operational_all/Files/'
path_to_sql_operational_folder = f'{path_to_file_airflow}arhive/'
path_to_sql_operational_folder2 = f'{path_to_file_airflow}operational/'

# Пути к файлам на сервере dbs
dbs_operational = '/scripts fsp/Current Files/Накопительный по РО/'
dbs_operational_etv = '/scripts fsp/Current Files/'
dbs_operational_autofilling= '/scripts fsp/Current Files/Накопительный по автозаливкам/'
dbs_operational_dozvon= '/scripts fsp/Current Files/Накопительный по дозвонам/'



# Выполнение заданий
sql_etv = PythonOperator(
    task_id='etv_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_etv, 'path_csv_file': path_to_sql_operational_folder2, 'name_csv_file': file_name_etv},
    dag=dag
    )

sql_autofilling = PythonOperator(
    task_id='autofilling_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_autofilling, 'path_csv_file': path_to_sql_operational_folder, 'name_csv_file': file_name_autofilling},
    dag=dag
    )


# Блок отправки обработка оперативного архива и отправка файлов в clickhouse.

update_operational_accumulate= PythonOperator(
    task_id='update_operational_accumulate', 
    python_callable = update_operational, 
    op_kwargs={'n': n,  'stop': stop, 'path_to_sql' : sql_operational, 'file_name' : file_name_operational, 
               'path_to_airflow' : path_to_sql_operational_folder, 'file_deleted' : file_name_autofilling, 'path_del' : path_to_sql_operational_folder}, 
    dag=dag
    )



# Блок отправки всех файлов в папку DBS.
transfer_etv = PythonOperator(
    task_id='transfer_etv', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_sql_operational_folder2, 'to_path': dbs_operational_etv, 'file': file_name_etv, 'db': 'DBS'}, 
    dag=dag
    )

transfer_autofilling = PythonOperator(
    task_id='transfer_autofilling', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_sql_operational_folder, 'to_path': dbs_operational_autofilling, 'file': file_name_autofilling, 'db': 'DBS'}, 
    dag=dag
    )

transfer_dozvon = PythonOperator(
    task_id='transfer_dozvon', 
    python_callable=transfer_dozvon_to_dbs,
    op_kwargs={'from_path': path_to_sql_operational_folder2, 'file': file_name_dozvon, 'to_path': dbs_operational_dozvon,  'file_result' : file_name_dozvon_per_day, 'db': 'DBS'}, 
    dag=dag
    )

transfer_operational = PythonOperator(
    task_id='transfer_operational', 
    python_callable=transfer_files_to_dbs, 
    op_kwargs={'from_path': path_to_sql_operational_folder, 'to_path': dbs_operational, 'db': 'DBS'}, 
    dag=dag
    )



# Блок отправки  файлов в clickhouse.

transfer_dozvon_to_click = PythonOperator(
    task_id='transfer_dozvon_to_click', 
    python_callable=transfer_files_to_click, 
    op_kwargs={'path_to_file': path_to_sql_operational_folder2, 'files': file_name_dozvon}, 
    dag=dag
    )

transfer_etv_to_click = PythonOperator(
    task_id='transfer_etv_to_click', 
    python_callable=etv_universal, 
    # op_kwargs={}, 
    dag=dag
    )

# Очистка папки
clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder, 
    op_kwargs={'folder': path_to_sql_operational_folder}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
    task_id='send_telegram_message',
    telegram_conn_id='Telegram',
    chat_id='-1001412983860',
    text='Случилась ошибочка в Накопительном РО :(',
    dag=dag,
    trigger_rule='one_failed'
)


sql_etv >> transfer_etv >> transfer_dozvon >> transfer_dozvon_to_click >> transfer_etv_to_click >> send_telegram_message
sql_autofilling >> transfer_autofilling >> update_operational_accumulate >> transfer_operational >> clear_folders >> send_telegram_message
