import pendulum
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.transfer_files_to_dbs import transfer_file_to_dbs
from fsp.transfer_files_to_dbs import transfer_files_to_dbs
from project_defenition.project_teams import project_teams
from project_defenition.project_queues import project_queues
from project_defenition.project_steps import project_steps
from project_defenition.excel_stavki_fsp import excel_stavki_fsp
from project_defenition.excel_teams import excel_teams
from project_defenition.excel_queues import excel_queues


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
excel_path_dbs = '/Отчеты BI/Стандартные справочники/'


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

stavki_fsp_excel = PythonOperator(
    task_id='stavki_fsp_excel', 
    python_callable=excel_stavki_fsp,
    dag=dag
    )

teams_excel = PythonOperator(
    task_id='teams_excel', 
    python_callable=excel_teams,
    dag=dag
    )

queues_excel = PythonOperator(
    task_id='queues_excel', 
    python_callable=excel_queues,
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

stavki_and_teams_excel_transfer = PythonOperator(
    task_id='stavki_and_teams_excel_transfer', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': excel_path_dbs, 'db': 'DBS', 'file1': 'Ставки ФСП.xlsx', 'file2': 'Команды_Проекты.xlsx'}, 
    dag=dag
    )

queues_excel_transfer = PythonOperator(
    task_id='queues_excel_transfer', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': excel_path_dbs, 'db': 'DBS', 'file1': 'Группировка очередей.xlsx', 'file2': ''}, 
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
[stavki_fsp_excel,teams_excel] >> stavki_and_teams_excel_transfer
queues_excel >> queues_excel_transfer
[project_steps_transfer, project_queues_transfer, project_teams_transfer,stavki_and_teams_excel_transfer,queues_excel_transfer,queues_excel_transfer] >> send_telegram_message