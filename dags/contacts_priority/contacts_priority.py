import pendulum
import dateutil.relativedelta
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from contacts_priority.transfer_contacts_to_click import transfer_to_click
from contacts_priority.to_temp_table import temp_table
from contacts_priority.setting_priorities import setting_priorities


default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }


dag = DAG(
    dag_id='contacts_priority',
    schedule_interval='0 22 * * FRI',
    start_date=pendulum.datetime(2023, 8, 22, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud_name = 'cloud_128'

x = 0
y = 10000000
stop = 0

# Выполнение заданий
transfer_contacts_to_click = PythonOperator(
    task_id='transfer_contacts_to_click', 
    python_callable = transfer_to_click, 
    op_kwargs={'x': x, 'y': y, 'stop': stop}, 
    dag=dag
    )

create_temp_table = PythonOperator(
    task_id='create_temp_table', 
    python_callable = temp_table, 
    # op_kwargs={'x': x, 'y': y, 'stop': stop}, 
    dag=dag
    )

set_priorities = PythonOperator(
    task_id='setting_priorities', 
    python_callable = setting_priorities, 
    # op_kwargs={'x': x, 'y': y, 'stop': stop}, 
    dag=dag
    )


# # Отправка уведомления об ошибке в Telegram.
# send_telegram_message = TelegramOperator(
#     task_id='send_telegram_message',
#     telegram_conn_id='Telegram',
#     chat_id='-1001412983860',
#     text='Ошибка в модуле при выгрузке данных для оперативных отчетов',
#     dag=dag,
#     trigger_rule='one_failed'
# )

# Очередности выполнения задач.
transfer_contacts_to_click >> create_temp_table >> set_priorities














