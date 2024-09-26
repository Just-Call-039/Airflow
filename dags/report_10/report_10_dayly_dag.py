# подключить библиотеки dag и создать задание
from datetime import timedelta
import pendulum
import datetime

from airflow import DAG
# from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from report_10.report_10_dayly_to_clickhouse import dayly_report_to_clickhouse


default_args = {
    'owner': 'Vitaliy Manetin',
    'email': 'poka@chto.net',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }


dag = DAG(
    dag_id='report_10_this_month_before_yest',
    schedule_interval= '40 5 * * *',
    start_date=pendulum.datetime(2024, 6, 15, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args,
    max_active_runs=1)
   
# date = str(now.replace(month=now.month - 1).date())
# date = str(datetime.date.today())
# year_num = date.split('-')[0]
# month_num = date.split('-')[1]
# # day_num = date.split('-')[2]
# file_name = f'Звонки_{year_num}_{month_num}.csv'
# path_to_file_airflow = '/root/airflow/dags/report_10/report_files/'

access_pass = '/root/airflow/dags/report_10/report_files/access.txt'


dag_dayly_report_to_clickhouse = PythonOperator(
    task_id='dag_dayly_report_to_clickhouse', 
    python_callable=dayly_report_to_clickhouse, 
    op_kwargs={'access_pass': access_pass}, 
    dag=dag
    )

# op_kwargs={'file_name': file_name, 'path_to_file_airflow': path_to_file_airflow, 'access_pass': access_pass},

dag_dayly_report_to_clickhouse