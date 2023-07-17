from datetime import timedelta
from datetime import datetime
# import datetime
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.repeat_download import sql_query_to_csv
from commons_sawa.telegram import telegram_send


default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='dispetchers_4_50',
    schedule_interval='50 4 * * *',
    start_date=pendulum.datetime(2022, 11, 22, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

n = '1'
days = 1
cloud_name = 'cloud_128'

token = '5232984306:AAERQkP-trXpL4qbCivxAINX-Oz0oSL3hVY'
chat_id = 738716223  # your chat id

today_date = datetime.now().strftime("%d/%m/%Y")
text_leads = today_date+' Отправляем файл Лидов'
text_recalls = today_date+' Отправляем файл Перезвонов'
text_meetings = today_date+' Отправляем файл с Заявками'

path_to_sql = '/root/airflow/dags/dispetchers 4-50/SQL/'
sql_leads = f'{path_to_sql}Leads_4_50.sql'
sql_recalls = f'{path_to_sql}Recalls_4_50.sql'

path_to_file_sql_airflow = '/root/airflow/dags/dispetchers 4-50/Files/'
path_to_meetings = '/root/airflow/dags/fsp/Files/meetings/'

csv_leads = 'leads_4_50.csv'
csv_recalls = 'recalls_4_50.csv'
csv_meetings = 'meeting_phones.csv'

# Блок выполнения SQL запросов.
leads_sql = PythonOperator(
    task_id='leads_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_leads, 'path_csv_file': path_to_file_sql_airflow, 'name_csv_file': csv_leads}, 
    dag=dag
    )

recalls_sql = PythonOperator(
    task_id='recalls_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_recalls, 'path_csv_file': path_to_file_sql_airflow, 'name_csv_file': csv_recalls}, 
    dag=dag
    )

leads_telegram = PythonOperator(
    task_id='leads_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_leads, 'token': token, 'chat_id': chat_id, 'filepath': path_to_file_sql_airflow, 'filename': csv_leads}, 
    dag=dag
    )

recalls_telegram = PythonOperator(
    task_id='recalls_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_recalls, 'token': token, 'chat_id': chat_id, 'filepath': path_to_file_sql_airflow, 'filename': csv_recalls}, 
    dag=dag
    )

meetings_telegram = PythonOperator(
    task_id='meetings_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_meetings, 'token': token, 'chat_id': chat_id, 'filepath': path_to_meetings, 'filename': csv_meetings}, 
    dag=dag
    )

leads_sql >> leads_telegram
recalls_sql >> recalls_telegram
[recalls_telegram,leads_telegram] >> meetings_telegram
