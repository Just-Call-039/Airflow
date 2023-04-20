import pendulum
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.transfer_files_to_dbs import transfer_files_to_dbs
from project_defenition.project_teams import project_teams
from project_defenition.project_queues import project_queues
from project_defenition.project_steps import project_steps


default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='project_defenition',
    schedule_interval='20 23 * * *',
    start_date=pendulum.datetime(2023, 3, 13, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


# Пути к файлам на сервере airflow.
path_to_file_airflow = '/root/airflow/dags/project_defenition/projects/' # Сюда падают users и worktime
project_teams_path_airflow = f'{path_to_file_airflow}teams/'
project_queues_path_airflow = f'{path_to_file_airflow}queues/'
project_steps_path_airflow = f'{path_to_file_airflow}steps/'

# Пути к файлам на сервере dbs.
path_to_file_dbs = '/scripts fsp/Current Files/Проект/' # Сюда падают users и worktime
project_teams_path_dbs = f'{path_to_file_dbs}Команды/'
project_queues_path_dbs = f'{path_to_file_dbs}Очереди/'
project_steps_path_dbs = f'{path_to_file_dbs}Шаги/'


# Выгрузка файлов на airflow
project_teams_csv = PythonOperator(
    task_id='project_teams_csv', 
    python_callable=project_teams,
    dag=dag
    )

project_queues_csv = PythonOperator(
    task_id='project_queues_csv', 
    python_callable=project_queues,
    dag=dag
    )

project_steps_csv = PythonOperator(
    task_id='project_steps_csv', 
    python_callable=project_steps,
    dag=dag
    )


# Перенос всех файлов в папку DBS.
project_teams_transfer = PythonOperator(
    task_id='project_teams_transfer', 
    python_callable=transfer_files_to_dbs, 
    op_kwargs={'from_path': project_teams_path_airflow, 'to_path': project_teams_path_dbs, 'db': 'DBS'}, 
    dag=dag
    )

project_queues_transfer = PythonOperator(
    task_id='project_queues_transfer', 
    python_callable=transfer_files_to_dbs, 
    op_kwargs={'from_path': project_queues_path_airflow, 'to_path': project_queues_path_dbs, 'db': 'DBS'}, 
    dag=dag
    )

project_steps_transfer = PythonOperator(
    task_id='project_steps_transfer', 
    python_callable=transfer_files_to_dbs, 
    op_kwargs={'from_path': project_steps_path_airflow, 'to_path': project_steps_path_dbs, 'db': 'DBS'}, 
    dag=dag
    )


# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Ошибка логирования проектов',
        dag=dag,
        # on_failure_callback=True,
        # trigger_rule='all_success'
        trigger_rule='one_failed'
    )

# Очередности выполнения задач.
project_teams_csv >> project_teams_transfer
project_queues_csv >> project_queues_transfer
project_steps_csv >> project_steps_transfer
[project_steps_transfer, project_queues_transfer, project_teams_transfer] >> send_telegram_message