import pendulum
import dateutil.relativedelta
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from dialogs_errors.dialogs_errors_export import dialog_errors


default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }


dag = DAG(
    dag_id='dialog_errors',
    schedule_interval='20 09 * * *',
    start_date=pendulum.datetime(2023, 9, 11, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )



# Выполнение заданий
dialog_errors = PythonOperator(
    task_id='dialog_errors', 
    python_callable = dialog_errors, 
    dag=dag
    )

send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Ошибки РО выгружены',
        dag=dag
    )

dialog_errors >> send_telegram_message