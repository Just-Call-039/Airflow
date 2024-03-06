import pendulum
import dateutil.relativedelta
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.transfer_files_to_dbs import transfer_files_to_dbs
from fsp.transfer_files_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv


from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
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
    schedule_interval='0 7-19/2 * * *',
    start_date=pendulum.datetime(2023, 4, 24, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud_name = 'cloud_128'
# cloud_name = '72'

# Пути к sql запросам на сервере airflow
path_to_sql_airflow = '/root/airflow/dags/operational_all/SQL/'
sql_operational = f'{path_to_sql_airflow}operational.sql'
sql_operational_calls = f'{path_to_sql_airflow}operational_calls.sql'
sql_worktime = f'{path_to_sql_airflow}worktime.sql'
sql_transfers = f'{path_to_sql_airflow}transfers.sql'
sql_meetings = f'{path_to_sql_airflow}meetings.sql'
sql_etv = f'{path_to_sql_airflow}etv.sql'
sql_autofilling = f'{path_to_sql_airflow}autofilling.sql'


# Наименование файлов
file_name_users = 'users.csv'
# file_name_operational = 'operational.csv'
# file_name_operational_calls = 'operational_calls.csv'
# file_name_worktime = 'worktime.csv'
# file_name_transfers = 'transfers.csv'
# file_name_meetings = 'meetings.csv'
# file_name_etv = 'etv.csv'
# file_name_autofilling = 'autofilling.csv'

file_name_operational = 'Оперативный.csv'
file_name_operational_calls = 'Дозвон.csv'
file_name_worktime = 'Время.csv'
file_name_transfers = 'Оперативный_переводы.csv'
file_name_meetings = 'Оперативный_заявки.csv'
file_name_etv = 'ЕТВ.csv'
file_name_autofilling = 'Автозаливки.csv'


# Пути к файлам на сервере airflow
# Сразу после sql
path_to_file_airflow = '/root/airflow/dags/operational_all/Files/'
path_to_sql_operational_folder = f'{path_to_file_airflow}sql_operational/'
path_to_sql_operational_operativ = f'{path_to_file_airflow}x/'


# Путь к пользователям
path_to_file_users = '/root/airflow/dags/request_with_calls_today/Files/'

# После обработки питоном (итог)
path_to_operational_folder = f'{path_to_file_airflow}operational/'

# Пути к файлам на сервере dbs
path_to_file_dbs = '/operational/'
dbs_operational = f'{path_to_file_dbs}operational/'
dbs_operational2 = '/scripts fsp/Current Files/'
dbs_operational3 = '/scripts fsp/Current Files/'
# dbs_operational3 = '/test folder/'



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

sql_worktime = PythonOperator(
    task_id='worktime_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_worktime, 'path_csv_file': path_to_operational_folder, 'name_csv_file': file_name_worktime},
    dag=dag
    )

sql_transfers = PythonOperator(
    task_id='transfers_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_transfers, 'path_csv_file': path_to_operational_folder, 'name_csv_file': file_name_transfers},
    dag=dag
    )

sql_meetings = PythonOperator(
    task_id='meetings_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_meetings, 'path_csv_file': path_to_operational_folder, 'name_csv_file': file_name_meetings},
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
                'name_calls': file_name_operational, 'path_to_final_folder': path_to_sql_operational_operativ}, 
    dag=dag
    )

transformation_operational_calls = PythonOperator(
    task_id='operational_calls_transformation', 
    python_callable = operational_calls_transformation, 
    op_kwargs={'path_to_folder': path_to_sql_operational_folder, 'name_calls': file_name_operational_calls, 'path_to_final_folder': path_to_operational_folder}, 
    dag=dag
    )


# Перенос всех файлов в папку DBS.
# transfer_operational = PythonOperator(
#     task_id='operational_transfer', 
#     python_callable=transfer_files_to_dbs, 
#     op_kwargs={'from_path': path_to_operational_folder, 'to_path': dbs_operational, 'db': 'DBS'}, 
#     dag=dag
#     )

transfer_operational2 = PythonOperator(
    task_id='operational_transfer2', 
    python_callable=transfer_files_to_dbs, 
    op_kwargs={'from_path': path_to_operational_folder, 'to_path': dbs_operational2, 'db': 'DBS'}, 
    dag=dag
    )

transfer_operational_main = PythonOperator(
    task_id='operational_transfer_main', 
    python_callable=transfer_files_to_dbs, 
    op_kwargs={'from_path': path_to_sql_operational_operativ, 'to_path': dbs_operational3, 'db': 'DBS'}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
    task_id='send_telegram_message',
    telegram_conn_id='Telegram',
    chat_id='-1001412983860',
    text='Ошибка в модуле при выгрузке данных для оперативных отчетов',
    dag=dag,
    trigger_rule='one_failed'
)

# Очередности выполнения задач.

sql_operational >> transformation_operational >> transfer_operational_main
sql_operational_calls >> transformation_operational_calls

[transformation_operational_calls, sql_worktime,
  sql_transfers, sql_meetings, sql_etv, sql_autofilling] >> transfer_operational2


[transfer_operational_main, transfer_operational2] >> send_telegram_message














