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

n = 1

access_pass = '/root/airflow/dags/report_10/report_files/access.txt'


dag_dayly_report_to_clickhouse = PythonOperator(
    task_id='dag_dayly_report_to_clickhouse', 
    python_callable=dayly_report_to_clickhouse, 
    op_kwargs={'access_pass': access_pass,
               'n' : n}, 
    dag=dag
    )

# dag_temperaly = PythonOperator(
#     task_id='dag_temperaly', 
#     python_callable=dag_temp, 
#     op_kwargs={'access_pass': access_pass,
#                'n' : n}, 
#     dag=dag
#     )

dag_dayly_report_to_clickhouse 