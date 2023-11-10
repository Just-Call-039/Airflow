import pendulum
import dateutil.relativedelta
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from address_log.transfer_addresses_to_click import transfer_to_click


default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }


dag = DAG(
    dag_id='address_log',
    schedule_interval='40 22 * * *',
    start_date=pendulum.datetime(2023, 10, 17, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud_name = 'cloud_128'

x = 0
y = 10000000
stop = 0

path_sql = '/root/airflow/dags/address_log/SQL'
sql_general_create = f'{path_sql}/general_create.sql'


# Выполнение заданий
transfer_addresses_to_click = PythonOperator(
    task_id='transfer_addresses_to_click', 
    python_callable = transfer_to_click, 
    op_kwargs={'x': x, 'y': y, 'stop': stop, 'general_create' : sql_general_create}, 
    dag=dag
    )


# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Адреса залиты',
        dag=dag
    )


transfer_addresses_to_click >> send_telegram_message