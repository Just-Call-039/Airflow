import pendulum
import dateutil.relativedelta
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from gar_test.gar_into_click import gar_into_click


default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }


dag = DAG(
    dag_id='gar',
    schedule_interval='40 22 * * *',
    start_date=pendulum.datetime(2023, 10, 23, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


path = '/gar_xml.zip'


# Выполнение заданий
gar_into_click = PythonOperator(
    task_id='gar_into_click', 
    python_callable = gar_into_click, 
    op_kwargs={'zip_path': path}, 
    dag=dag
    )


# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='ГАР залиты',
        dag=dag
    )


gar_into_click >> send_telegram_message