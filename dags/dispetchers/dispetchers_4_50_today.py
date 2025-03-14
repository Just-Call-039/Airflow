from datetime import timedelta
from datetime import datetime
# import datetime
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.repeat_download import sql_query_to_csv
from commons_sawa.telegram import telegram_send
from dispetchers.disp_editer_copy import disp_editors
from dispetchers import waiter



default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }


dag = DAG(
    dag_id='dispetchers_4_50_today',
    schedule_interval='30 22 * * *',
    start_date=pendulum.datetime(2022, 11, 22, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

n = '1'
days = 1
# cloud_name = 'cloud_128'
cloud_name = 'cloud_183'


token = '5232984306:AAERQkP-trXpL4qbCivxAINX-Oz0oSL3hVY'
chat_id = 738716223 
nastya_chat_id = 1680452690 

today_date = datetime.now().strftime("%d/%m/%Y")
text_leads = today_date+' Отправляем файл Лидов'
text_waiter = today_date+' Отправляем файл Ждуны'
text_recalls = today_date+' Отправляем файл Перезвонов'
text_transfers = 'Отправляем файл с переводами за вчера'
text_meetings = today_date+' Отправляем файл с Заявками за три месяца'

path_to_sql = '/root/airflow/dags/dispetchers/SQL_today/'
sql_leads = f'{path_to_sql}Leads_4_50.sql'
sql_recalls = f'{path_to_sql}Recalls_4_50.sql'
sql_transfers = f'{path_to_sql}Transfers.sql'
sql_request = f'{path_to_sql}Request_Ksusha.sql'
sql_waiter = f'{path_to_sql}Waiters.sql'

path_to_file_sql_airflow = '/root/airflow/dags/dispetchers/Files/'

csv_leads = 'leads_4_50_today.csv'
csv_recalls = 'recalls_4_50_today.csv'
csv_transfers = 'transfers_today.csv'
csv_meetings = 'meeting_today.csv'
csv_waiter = 'waiter.csv'
csv_result_waiter = 'waiter_result.xlsx'

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

transfers_sql = PythonOperator(
    task_id='transfers_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_transfers, 'path_csv_file': path_to_file_sql_airflow, 'name_csv_file': csv_transfers}, 
    dag=dag
    )

request_sql = PythonOperator(
    task_id='request_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_request, 'path_csv_file': path_to_file_sql_airflow, 'name_csv_file': csv_meetings}, 
    dag=dag
    )

waiter_sql = PythonOperator(
    task_id='waiter_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_waiter, 'path_csv_file': path_to_file_sql_airflow, 'name_csv_file': csv_waiter}, 
    dag=dag
    )

waiter_editor = PythonOperator(
    task_id='waiter_editor', 
    python_callable=waiter.set_project, 
    op_kwargs={'waiter_path': f'{path_to_file_sql_airflow}{csv_waiter}', 
               'result_path': f'{path_to_file_sql_airflow}{csv_result_waiter}'}, 
    dag=dag
    )


lids_upgrade = PythonOperator(
    task_id='lids_upgrade', 
    python_callable=disp_editors, 
    op_kwargs={'path_to_files': path_to_file_sql_airflow, 'lids': csv_leads, 'path_result': path_to_file_sql_airflow}, 
    dag=dag
    )

leads_telegram = PythonOperator(
    task_id='leads_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_leads, 'token': token, 'chat_id': chat_id, 'filepath': path_to_file_sql_airflow, 'filename': csv_leads}, 
    dag=dag
    )

waiter_telegram = PythonOperator(
    task_id='waiter_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_waiter, 'token': token, 'chat_id': nastya_chat_id, 'filepath': path_to_file_sql_airflow, 'filename': csv_result_waiter}, 
    dag=dag
    )

recalls_telegram = PythonOperator(
    task_id='recalls_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_recalls, 'token': token, 'chat_id': chat_id, 'filepath': path_to_file_sql_airflow, 'filename': csv_recalls}, 
    dag=dag
    )




transfers_telegram = PythonOperator(
    task_id='transfers_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_transfers, 'token': token, 'chat_id': chat_id, 'filepath': path_to_file_sql_airflow, 'filename': csv_transfers}, 
    dag=dag
    )


meetings_telegram = PythonOperator(
     task_id='meetings_telegram', 
     python_callable=telegram_send, 
     op_kwargs={'text': text_meetings, 'token': token, 'chat_id': chat_id, 'filepath': path_to_file_sql_airflow, 'filename': csv_meetings}, 
     dag=dag
     )

leads_sql >> lids_upgrade >> leads_telegram
recalls_sql >> recalls_telegram
waiter_sql >> waiter_editor >> waiter_telegram
transfers_sql >> transfers_telegram
request_sql >> meetings_telegram
