import pendulum
import dateutil.relativedelta
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.repeat_download import repeat_download
from fsp.transfer_files_to_dbs import transfer_files_to_dbs
from fsp.transfer_files_to_dbs import remove_files_from_airflow
from truby.truby_editing import truby_transformation


default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='truby',
    schedule_interval='40 6 * * *',
    start_date=pendulum.datetime(2023, 5, 16, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

n = 1
n30 = 30
days = 3
source = [7,8,9]
cloud_truby = 'Truby'
cloud_name = 'cloud_128'

# Пути к sql запросам на сервере airflow.
path_to_sql_airflow = '/root/airflow/dags/truby/SQL/'
sql_file_truby = f'{path_to_sql_airflow}truby.sql'
sql_file_robot_log = f'{path_to_sql_airflow}robot_log.sql'
sql_file_leg_log = f'{path_to_sql_airflow}leg_log.sql'
sql_file_leg_log_30 = f'{path_to_sql_airflow}leg_log_30.sql'
sql_file_truby_30 = f'{path_to_sql_airflow}truby_30.sql'

# Наименование файлов
file_name_truby = 'gate_{}.csv'
file_name_robot_log = 'robot_log_{}.csv'
file_name_leg_log = 'leg_log_{}.csv'
file_name_autootvetchiky = 'Автоответчики_трафик_{}.csv'
file_name_leg_log_30 = 'LEG.csv'
file_name_truby_30 = 'gate_{}.csv'

# Пути к файлам на сервере airflow.
# Сразу после sql
path_to_file_airflow = '/root/airflow/dags/truby/Files/'
path_to_sql_truby_folder = f'{path_to_file_airflow}sql_truby/'
path_to_sql_robot_log_folder = f'{path_to_file_airflow}sql_robot_log/'
path_to_sql_leg_log_folder = f'{path_to_file_airflow}sql_leg_log/'

# После обработки питоном (итог)
path_to_autootvetchiky_folder = f'{path_to_file_airflow}autootvetchiky/'
path_to_leg_log_folder_30 = f'{path_to_file_airflow}leg_log/'
path_to_truby_folder_30 = f'{path_to_file_airflow}truby/'


# Пути к файлам на сервере dbs.
path_to_file_dbs = '/dbs/scripts fsp/Current Files/'
dbs_autootvet = f'{path_to_file_dbs}Автоответчики/'
dbs_truby_30 = f'{path_to_file_dbs}Трубы/'


# Выполнение SQL запросов.
sql_truby = PythonOperator(
    task_id='truby_sql',
    python_callable=repeat_download,
    op_kwargs={'n': n, 'days': days, 'source': source, 'cloud': cloud_truby, 'path_sql_file': sql_file_truby,
                'path_csv_file': path_to_sql_truby_folder, 'name_csv_file': file_name_truby},
    dag=dag
    )

sql_robot_log = PythonOperator(
    task_id='robot_log_sql',
    python_callable=repeat_download,
    op_kwargs={'n': n, 'days': days, 'cloud': cloud_name, 'path_sql_file': sql_file_robot_log,
                'path_csv_file': path_to_sql_robot_log_folder, 'name_csv_file': file_name_robot_log, 'source': source},
    dag=dag
    )

sql_leg_log = PythonOperator(
    task_id='leg_log_sql',
    python_callable=repeat_download,
    op_kwargs={'n': n, 'days': days, 'cloud': cloud_name, 'path_sql_file': sql_file_leg_log,
                'path_csv_file': path_to_sql_leg_log_folder, 'name_csv_file': file_name_leg_log, 'source': source},
    dag=dag
    )

sql_leg_log_30 = PythonOperator(
    task_id='30_leg_log_sql',
    python_callable=repeat_download,
    op_kwargs={'n': n30, 'days': 1, 'cloud': cloud_name, 'path_sql_file': sql_file_leg_log_30,
                'path_csv_file': path_to_leg_log_folder_30, 'name_csv_file': file_name_leg_log_30, 'source': source},
    dag=dag
    )

sql_truby_30 = PythonOperator(
    task_id='30_truby_sql',
    python_callable=repeat_download,
    op_kwargs={'n': n30, 'days': 1, 'source': source, 'cloud': cloud_truby, 'path_sql_file': sql_file_truby_30,
                'path_csv_file': path_to_truby_folder_30, 'name_csv_file': file_name_truby_30},
    dag=dag
    )


# Преобразование файлов после sql.
transformation_autootvetchiky = PythonOperator(
    task_id='truby_transformation', 
    python_callable= truby_transformation, 
    op_kwargs={'n': n, 'days': days, 'rl_path': path_to_sql_robot_log_folder, 'll_path':path_to_sql_leg_log_folder,
                'truby_path': path_to_sql_truby_folder, 'full_data_path': path_to_autootvetchiky_folder, 'full_data_name': file_name_autootvetchiky}, 
    dag=dag
    )


# Перенос всех файлов в папку DBS.
transfer_autootvetchiky = PythonOperator(
    task_id='autootvetchiky_transfer', 
    python_callable=transfer_files_to_dbs, 
    op_kwargs={'from_path': path_to_autootvetchiky_folder, 'to_path': dbs_autootvet, 'db': 'DBS'}, 
    dag=dag
    )

transfer_leg_log = PythonOperator(
    task_id='leg_log_30_transfer', 
    python_callable=transfer_files_to_dbs, 
    op_kwargs={'from_path': path_to_leg_log_folder_30, 'to_path': path_to_file_dbs, 'db': 'DBS'}, 
    dag=dag
    )

transfer_truby = PythonOperator(
    task_id='truby_30_transfer', 
    python_callable=transfer_files_to_dbs, 
    op_kwargs={'from_path': path_to_truby_folder_30, 'to_path': dbs_truby_30, 'db': 'DBS'}, 
    dag=dag
    )


# remove_files_from_airflow = PythonOperator(
#     task_id='remove_files_from_airflow', 
#     python_callable=remove_files_from_airflow, 
#     op_kwargs={'paths': [path_to_sql_truby_folder, path_to_sql_robot_log_folder, path_to_sql_leg_log_folder,
#                           path_to_autootvetchiky_folder, path_to_leg_log_folder_30, path_to_truby_folder_30]}, 
#     dag=dag
#     )

# # Отправка уведомления об ошибке в Telegram.
# send_telegram_message = TelegramOperator(
#         task_id='send_telegram_message',
#         telegram_conn_id='Telegram',
#         chat_id='-1001412983860',
#         text='Ошибка выгрузки данных для фсп',
#         dag=dag,
#         # on_failure_callback=True,
#         # trigger_rule='all_success'
#         trigger_rule='one_failed'
#     )

# Очередности выполнения задач.
[sql_truby,
sql_robot_log,
sql_leg_log] >> transformation_autootvetchiky >> transfer_autootvetchiky
sql_truby_30 >> transfer_truby
sql_leg_log_30 >> transfer_leg_log
# [transfer_autootvetchiky, transfer_truby, transfer_leg_log] >> remove_files_from_airflow >> send_telegram_message




















