import pendulum
import dateutil.relativedelta
from datetime import timedelta, date
import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from contacts_priority.priority_providers import priority_providers
from contacts_priority.transfer_contacts_to_click import transfer_to_click
from contacts_priority.to_temp_table import temp_table
from contacts_priority.setting_priorities_ptv import setting_priorities_ptv
from contacts_priority.setting_priorities_holod import setting_priorities_holod
from contacts_priority.create_priority_table import create_priority_table


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
    schedule_interval='0 16 * * FRI',
    start_date=pendulum.datetime(2023, 9, 11, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud_name = 'cloud_128'

x = 0
y = 10000000
stop = 0

priority_providers_file = '/root/airflow/dags/contacts_priority/Files/priorities.csv'

path_sql = '/root/airflow/dags/contacts_priority/SQL'

sql_general_create = f'{path_sql}/general_create.sql'
sql_temp_create = f'{path_sql}/temp_create.sql'
sql_temp_insert = f'{path_sql}/temp_insert.sql'
sql_settings_priorities_ptv = f'{path_sql}/settings_priorities_ptv.sql'
sql_settings_priorities_holod = f'{path_sql}/settings_priorities_holod.sql'




# Выполнение заданий
transfer_contacts_to_click = PythonOperator(
    task_id='transfer_contacts_to_click', 
    python_callable = transfer_to_click, 
    op_kwargs={'x': x, 'y': y, 'stop': stop, 'general_create' : sql_general_create}, 
    dag=dag
    )

priority_providers_table = PythonOperator(
    task_id='priority_providers_table', 
    python_callable = priority_providers, 
    op_kwargs={'file': priority_providers_file}, 
    dag=dag
    )

create_priority_table = PythonOperator(
    task_id='create_priority_table', 
    python_callable = create_priority_table, 
    dag=dag
    )

create_temp_table = PythonOperator(
    task_id='create_temp_table', 
    python_callable = temp_table, 
    op_kwargs={'temp_create': sql_temp_create, 'temp_insert': sql_temp_insert}, 
    dag=dag
    )

set_priorities_ptv = PythonOperator(
    task_id='setting_priorities_ptv', 
    python_callable = setting_priorities_ptv, 
    op_kwargs={'path_sql_file': sql_settings_priorities_ptv}, 
    dag=dag
    )

set_priorities_holod = PythonOperator(
    task_id='setting_priorities_holod', 
    python_callable = setting_priorities_holod, 
    op_kwargs={'path_sql_file': sql_settings_priorities_holod}, 
    dag=dag
    )


# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Новые приоритеты выставлены',
        dag=dag
    )

# Очередности выполнения задач.

# create_temp_table >> create_priority_table
[transfer_contacts_to_click >> create_temp_table] >> create_priority_table
priority_providers_table >> create_priority_table
create_priority_table >> set_priorities_ptv >> send_telegram_message
create_priority_table >> set_priorities_holod >> send_telegram_message












