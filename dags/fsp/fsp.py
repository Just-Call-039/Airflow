import pendulum
import dateutil.relativedelta
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.transfer_files_to_dbs import transfer_files_to_dbs
from fsp.transfer_files_to_dbs import transfer_file_to_dbs
from fsp.transfer_files_to_dbs import remove_files_from_airflow
from fsp.repeat_download import sql_query_to_csv
from fsp.repeat_download import repeat_download
from fsp.operator_calls_editing import operator_calls_transformation
from fsp.meetings_editing import meetings_transformation
from fsp.robotlog_calls_editing import robotlog_calls_transformation


default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='fsp_dag',
    schedule_interval='35 4 * * *',
    start_date=pendulum.datetime(2023, 3, 13, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

# Кол-во дней для выгрузки
days = 3

# Дата выгрузки (n = кол-во дней назад)
n = 1
now = datetime.datetime.now() - datetime.timedelta(days=n)

# Пути к sql запросам на сервере airflow.
path_to_sql_airflow = '/root/airflow/dags/fsp/SQL/'
sql_operator_calls = f'{path_to_sql_airflow}operator_calls.sql'
sql_meetings = f'{path_to_sql_airflow}meetings.sql'
sql_meetings_calls = f'{path_to_sql_airflow}meetings_calls.sql'
sql_robotlog_calls = f'{path_to_sql_airflow}robotlog_calls.sql'
sql_users = f'{path_to_sql_airflow}users.sql'
sql_worktime = f'{path_to_sql_airflow}worktime.sql'
sql_steps = f'{path_to_sql_airflow}steps.sql'

# Наименование файлов
file_name_operator_calls = 'calls_{}.csv'
file_name_users = 'users.csv'
file_name_robotlog_calls = 'robotlog_calls_{}.csv'
file_name_meetings_calls = 'meetings_calls.csv'
file_name_meetings = 'meetings.csv'
file_name_meeting_phones = 'meeting_phones.csv'
file_name_worktime = 'worktime.csv'
file_name_steps = 'steps_today.csv'

path_to_steps_files = '/root/airflow/dags/project_defenition/projects/steps'


# Пути к файлам на сервере airflow.
# Сразу после sql
path_to_file_airflow = '/root/airflow/dags/fsp/Files/' # Сюда падают users и worktime
path_to_sql_operator_calls_folder = f'{path_to_file_airflow}sql_operator_calls/'
path_to_sql_meetings_folder = f'{path_to_file_airflow}sql_meetings/'
path_to_sql_robotlog_calls_folder = f'{path_to_file_airflow}sql_robotlog_calls/'

# После обработки питоном (итог)
path_to_operator_calls_folder = f'{path_to_file_airflow}operator_calls/'
path_to_meetings_folder = f'{path_to_file_airflow}meetings/'
path_to_robotlog_calls_folder = f'{path_to_file_airflow}robotlog_calls/'

# Пути к файлам на сервере dbs.
path_to_file_dbs = '/fsp/' # Сюда падают users и worktime
dbs_operator_calls = f'{path_to_file_dbs}operator_calls/'
dbs_robotlog_calls = f'{path_to_file_dbs}robotlog_calls/'
dbs_meetings = f'{path_to_file_dbs}meetings/'

# Выполнение SQL запросов.
# sql_operator_calls = PythonOperator(
#     task_id='operator_calls_sql',
#     python_callable=repeat_download,
#     op_kwargs={'n': n, 'days': days, 'cloud': cloud_name, 'path_sql_file': sql_operator_calls, 'path_csv_file': path_to_sql_operator_calls_folder, 'name_csv_file': file_name_operator_calls, 'source': ''},
#     dag=dag
#     )

# sql_robotlog_calls = PythonOperator(
#     task_id='robotlog_calls_sql',
#     python_callable=repeat_download,
#     op_kwargs={'n': n, 'days': days, 'cloud': cloud_name, 'path_sql_file': sql_robotlog_calls, 'path_csv_file': path_to_sql_robotlog_calls_folder,
#                 'name_csv_file': file_name_robotlog_calls, 'source': ''},
#     dag=dag
#     )

sql_meetings_calls = PythonOperator(
    task_id='meetings_calls_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_meetings_calls, 'path_csv_file': path_to_sql_meetings_folder, 'name_csv_file': file_name_meetings_calls},
    dag=dag
    )

sql_meetings = PythonOperator(
    task_id='meetings_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_meetings, 'path_csv_file': path_to_sql_meetings_folder, 'name_csv_file': file_name_meetings},
    dag=dag
    )

sql_users = PythonOperator(
    task_id='users_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_users, 'path_csv_file': path_to_file_airflow, 'name_csv_file': file_name_users},
    dag=dag
    )

sql_worktime = PythonOperator(
    task_id='worktime_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_worktime, 'path_csv_file': path_to_file_airflow, 'name_csv_file': file_name_worktime},
    dag=dag
    )

sql_steps = PythonOperator(
    task_id='steps_sql',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_steps, 'path_csv_file': path_to_steps_files, 'name_csv_file': file_name_steps},
    dag=dag
    )


# Преобразование файлов после sql.
# transformation_operator_calls = PythonOperator(
#     task_id='operator_calls_transformation', 
#     python_callable=operator_calls_transformation, 
#     op_kwargs={'n': n, 'days': days, 'files_from_sql': path_to_sql_operator_calls_folder, 'main_folder': path_to_operator_calls_folder,
#                 'path_to_users': path_to_file_airflow, 'name_users': file_name_users}, 
#     dag=dag
#     )

# transformation_robotlog_calls = PythonOperator(
#     task_id='robotlog_calls_transformation', 
#     python_callable=robotlog_calls_transformation, 
#     op_kwargs={'n': n, 'days': days, 'files_from_sql': path_to_sql_robotlog_calls_folder, 'main_folder': path_to_robotlog_calls_folder,
#                 'path_to_users': path_to_file_airflow, 'name_users': file_name_users}, 
#     dag=dag
#     )

transformation_meetings = PythonOperator(
    task_id='meetings_transformation', 
    python_callable=meetings_transformation, 
    op_kwargs={'path_to_users': path_to_file_airflow, 'name_users': file_name_users,
                'path_to_folder': path_to_sql_meetings_folder, 'name_calls': file_name_meetings_calls, 'name_meetings': file_name_meetings,
                'path_to_final_folder': path_to_meetings_folder, 'name_phone_meetings': file_name_meeting_phones},
    dag=dag
    )


# Перенос всех файлов в папку DBS.
# transfer_operator_calls = PythonOperator(
#     task_id='operator_calls_transfer', 
#     python_callable=transfer_files_to_dbs, 
#     op_kwargs={'from_path': path_to_operator_calls_folder, 'to_path': dbs_operator_calls, 'db': 'DBS'}, 
#     dag=dag
#     )

# transfer_robotlog_calls = PythonOperator(
#     task_id='robotlog_calls_transfer', 
#     python_callable=transfer_files_to_dbs, 
#     op_kwargs={'from_path': path_to_robotlog_calls_folder, 'to_path': dbs_robotlog_calls, 'db': 'DBS'}, 
#     dag=dag
#     )

# transfer_meetings = PythonOperator(
#     task_id='meetings_transfer', 
#     python_callable=transfer_files_to_dbs, 
#     op_kwargs={'from_path': path_to_meetings_folder, 'to_path': dbs_meetings, 'db': 'DBS'}, 
#     dag=dag
#     )

# transfer_else = PythonOperator(
#     task_id='else_transfer', 
#     python_callable=transfer_file_to_dbs, 
#     op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'db': 'DBS', 'file1': file_name_worktime, 'file2': file_name_users}, 
#     dag=dag
#     )

# remove_files_from_airflow = PythonOperator(
#     task_id='remove_files_from_airflow', 
#     python_callable=remove_files_from_airflow, 
#     op_kwargs={'paths': [path_to_robotlog_calls_folder, path_to_operator_calls_folder]}, 
#     dag=dag
#     )

# Отправка уведомления об ошибке в Telegram.
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
# [sql_users,
# sql_worktime,
# sql_meetings_calls,
# sql_meetings,
# sql_operator_calls,
# sql_robotlog_calls]

[sql_meetings_calls, sql_meetings, sql_users, sql_steps] >> transformation_meetings 
# >> transfer_meetings
# [sql_operator_calls,sql_users] >> transformation_operator_calls >> transfer_operator_calls
# [sql_robotlog_calls,sql_users] >> transformation_robotlog_calls >> transfer_robotlog_calls
# [sql_users, sql_worktime] >> transfer_else
# [transfer_operator_calls, transfer_robotlog_calls] >> remove_files_from_airflow

# [transfer_meetings, transfer_operator_calls,transfer_robotlog_calls, transfer_else, remove_files_from_airflow ] >> send_telegram_message



[sql_steps,
 sql_users,
 sql_worktime,
 sql_meetings_calls,
 sql_meetings]
















