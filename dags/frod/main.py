import pendulum
import datetime

from datetime import timedelta, date

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator
from frod import get_date, proccess, save_dbs, clear_folder

default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='frod',
    schedule_interval='50 1 * * *',
    start_date=pendulum.datetime(2023, 4, 24, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

# cloud_name = 'cloud_128'
cloud_name = 'cloud_183'

date_i = date.today() - datetime.timedelta(days=1)

year = date_i.year
month = date_i.month
day = date_i.day

# Путь к SQL запросам

contact_sql = '/root/airflow/dags/frod/SQL/contact.sql'
robotlog_sql = '/root/airflow/dags/frod/SQL/robotlog.sql'

# Названия csv файлов

contact_csv = f'contact_{month:02}_{day:02}.csv'
robotlog_csv = f'robotlog_{month:02}_{day:02}.csv'
frod_csv = f'frod_{month:02}_{day:02}.csv'
step_csv = f'steps_{year}_{month:02}_{day:02}.csv'

# Путь к файлам проекта

project_path = '/root/airflow/dags/frod/Files/'
result_path = '/root/airflow/dags/frod/Files/result/'
dbs_path = '/Отчеты BI/frod/frod_per_day/'
step_path = '/root/airflow/dags/project_defenition/projects/steps/'
city_path = '/root/airflow/dags/indicators_to_regions/Files/Город.csv'
quality_path = '/root/airflow/dags/current_month_yesterday/Files/Качество.csv'

# Выгрузка данных

contact_load = PythonOperator(
    task_id = 'contact_load', 
    python_callable = get_date.mysql_load,
    op_kwargs = {
                'cloud' : cloud_name, 
                'path_sql_file' : contact_sql,
                'path_csv_file' : project_path, 
                'name_csv_file' : contact_csv 
                }, 
    dag=dag
    )

robotlog_load = PythonOperator(
    task_id = 'robotlog_load', 
    python_callable = get_date.mysql_load,
    op_kwargs = {
                'cloud' : cloud_name, 
                'path_sql_file' : robotlog_sql,
                'path_csv_file' : project_path, 
                'name_csv_file' : robotlog_csv 
                }, 
    dag=dag
    )

# Джойним и подготавливаем данные

merge_date = PythonOperator(
    task_id = 'merge_date', 
    python_callable = proccess.merge_df,
    op_kwargs = {
                'project_path' : project_path, 
                'contact_csv' : contact_csv, 
                'robotlog_csv' : robotlog_csv,
                'result_path' : result_path,
                'frod_csv' : frod_csv,
                'step_path' : step_path,
                'step_csv' : step_csv,
                'city_path' : city_path,
                'quality_path' : quality_path
                }, 
    dag=dag
    )

# Отправка файла в dbs

send_dbs = PythonOperator(
    task_id = 'send_dbs', 
    python_callable = save_dbs.save_to_dbs,
    op_kwargs = {
                'path_from' : result_path, 
                'path_to' : dbs_path
                }, 
    dag=dag
    )

delete_file = PythonOperator(
    task_id = 'delete_file', 
    python_callable = clear_folder.clear_folder,
    op_kwargs = {'folder' : project_path}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
    task_id='send_telegram_message',
    telegram_conn_id='Telegram',
    chat_id='-1001412983860',
    text='Ошибка в модуле при выгрузке фродилок',
    dag=dag,
    trigger_rule='one_failed'
)

[contact_load, robotlog_load ] >> merge_date >> send_dbs >> delete_file >> send_telegram_message
