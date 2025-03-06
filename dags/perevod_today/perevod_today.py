from datetime import timedelta
from datetime import datetime
# import datetime
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from perevod_today.perevod_to_click import click_transfer

default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
    }


dag = DAG(
    dag_id='perevod_today',
    schedule_interval='0 21 * * *',
    start_date=pendulum.datetime(2024, 5, 28, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud_name = 'cloud_128'


# Блок выполнения SQL запросов.
transfer_to_click = PythonOperator(
    task_id='transfer_to_click', 
    python_callable=click_transfer, 
    dag=dag
    )

transfer_to_click 