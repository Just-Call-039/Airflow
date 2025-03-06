from datetime import timedelta
from datetime import datetime
# import datetime
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from bissnes_in_clickhouse.b2b_to_click import b2b

default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
    }


dag = DAG(
    dag_id='b2b',
    schedule_interval='0 6 * * *',
    start_date=pendulum.datetime(2024, 5, 28, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

# cloud_name = 'cloud_128'
cloud_name = 'cloud_183'


# Блок выполнения SQL запросов.
b2b_to_click = PythonOperator(
    task_id='b2b_to_click', 
    python_callable=b2b, 
    dag=dag
    )

b2b_to_click 
