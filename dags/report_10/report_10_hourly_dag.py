from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from report_10.report_10_hourly_to_clickhouse import hourly_report_to_clickhouse
# from airflow.providers.telegram.operators.telegram import TelegramOperator
#import datetime

default_args = {
    'owner': 'Vitaliy Manetin',
    'email': 'poka@chto.net',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='report_10_today',
    schedule_interval= '*/20 6-18 * * *', 
    start_date=pendulum.datetime(2024, 6, 15, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args,
    max_active_runs=1)
   
 # данные обновляются каждые 10 минут '@hourly',


# date = str(now.replace(month=now.month - 1).date())
# date = str(datetime.date.today())
# year_num = date.split('-')[0]
# month_num = date.split('-')[1]
# day_num = date.split('-')[2]
# file_name = f'Звонки_{year_num}_{month_num}.csv'

# path_to_file_airflow = '/root/airflow/dags/report_10/report_files/'
access_pass = '/root/airflow/dags/report_10/report_files/access.txt'


# формирование отчета 
dag_hourly_report_to_clickhouse = PythonOperator(
    task_id='dag_hourly_report_to_clickhouse', 
    python_callable=hourly_report_to_clickhouse, 
    op_kwargs={'access_pass': access_pass}, 
    dag=dag
    )


dag_hourly_report_to_clickhouse