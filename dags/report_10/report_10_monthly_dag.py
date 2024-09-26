from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from report_10.report_10_monthly_to_clickhouse_ import monthly_report_to_clickhouse
# import datetime
# from airflow.providers.telegram.operators.telegram import TelegramOperator



default_args = {
    'owner': 'Vitaliy Manetin',
    'email': 'poka@chto.net',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='report_10_one_month_before',
    schedule_interval='20 5 1 * *',
    start_date=pendulum.datetime(2024, 6, 14, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args,
    max_active_runs=1)


access_pass = '/root/airflow/dags/report_10/report_files/access.txt'

dag_monthly_report_to_clickhouse = PythonOperator(
    task_id='dag_monthly_report_to_clickhouse', 
    python_callable=monthly_report_to_clickhouse, 
    op_kwargs={'access_pass': access_pass}, 
    dag=dag
    )


dag_monthly_report_to_clickhouse