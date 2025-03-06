from datetime import timedelta
import datetime
# import datetime
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.repeat_download import sql_query_to_csv
from commons_sawa.telegram import telegram_send
from dispetchers.disp_editer import disp_editors
from dispetchers import transfer_today



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
# cloud_name = 'cloud_128'
cloud_name = 'cloud_183'


token = '5232984306:AAERQkP-trXpL4qbCivxAINX-Oz0oSL3hVY'
chat_id = 738716223
nastya_chat_id = 1680452690  
liza_chat_id = 974747353

date_i = datetime.date.today() -  datetime.timedelta(days=1)

today_date = datetime.date.today().strftime("%d/%m/%Y")
text_leads = today_date+' Отправляем файл Лидов'
text_recalls = today_date+' Отправляем файл Перезвонов'
text_transfers = 'Отправляем файл с переводами за вчера'
text_meetings = today_date+' Отправляем файл с Заявками за три месяца'
text_transfer = today_date+' Отправляем файл с переовдами за вчера'


path_to_sql = '/root/airflow/dags/dispetchers/SQL/'
sql_leads = f'{path_to_sql}Leads_4_50.sql'
sql_recalls = f'{path_to_sql}Recalls_4_50.sql'
sql_transfers = f'{path_to_sql}Transfers.sql'
sql_request = f'{path_to_sql}Request_Ksusha.sql'
sql_recalls_edit = f'{path_to_sql}Recalls_4_50_edit.sql'
sql_call = f'{path_to_sql}transfer_call.sql'


path_to_file_sql_airflow = '/root/airflow/dags/dispetchers/Files/'

csv_leads = 'leads_4_50.csv'
excel_lids = 'Лиды_4_50.xlsx'
csv_recalls = 'recalls_4_50.csv'
csv_transfers = 'transfers.csv'
csv_meetings = 'meeting.csv'
csv_transfer_call = 'transfer_call.csv'
csv_recalls_edit = 'recalls_4_50_edit.csv'
csv_recalls_edit1 = 'Перезвоны обработанный.xlsx'
csv_recalls_edit_xlsx = 'Перезвоны.xlsx'
transfer_xlsx = 'Переводы.xlsx'

step_path = '/root/airflow/dags/project_defenition/projects/steps/'



# Блок выполнения SQL запросов.
leads_sql = PythonOperator(
    task_id='leads_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_leads, 'path_csv_file': path_to_file_sql_airflow, 'name_csv_file': csv_leads}, 
    dag=dag
    )

transfer_call_sql = PythonOperator(
    task_id='transfer_call_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_call, 'path_csv_file': path_to_file_sql_airflow, 'name_csv_file': csv_transfer_call}, 
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

recalls_edit_sql = PythonOperator(
    task_id='recalls_edit_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_recalls_edit, 'path_csv_file': path_to_file_sql_airflow, 'name_csv_file': csv_recalls_edit}, 
    dag=dag
    )


lids_upgrade = PythonOperator(
    task_id='lids_upgrade', 
    python_callable=disp_editors, 
    op_kwargs={'path_to_files': path_to_file_sql_airflow, 'lids': csv_leads, 'path_result': path_to_file_sql_airflow, 'calls' : csv_recalls_edit}, 
    dag=dag
    )


transfer_edit = PythonOperator(
    task_id='transfer_edit', 
    python_callable=transfer_today.transfer_edit, 
    op_kwargs={'call_path': f'{path_to_file_sql_airflow}{csv_transfer_call}', 'step_path': step_path, 'date_i': date_i, 
               'result_path' : f'{path_to_file_sql_airflow}{transfer_xlsx}'}, 
    dag=dag
    )

leads_telegram = PythonOperator(
    task_id='leads_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_leads, 'token': token, 'chat_id': chat_id, 'filepath': path_to_file_sql_airflow, 'filename': csv_leads}, 
    dag=dag
    )

leads_telegram1 = PythonOperator(
    task_id='leads_telegram1', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_leads, 'token': token, 'chat_id': nastya_chat_id, 'filepath': path_to_file_sql_airflow, 'filename': excel_lids}, 
    dag=dag
    )


leads_liza_telegram1 = PythonOperator(
    task_id='leads_liza_telegram1', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_leads, 'token': token, 'chat_id': liza_chat_id, 'filepath': path_to_file_sql_airflow, 'filename': excel_lids}, 
    dag=dag
    )

transfer_call_telegram = PythonOperator(
    task_id='transfer_call_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_transfer, 'token': token, 'chat_id': nastya_chat_id, 'filepath': path_to_file_sql_airflow, 'filename': transfer_xlsx}, 
    dag=dag
    )

transfer_call_liza_telegram = PythonOperator(
    task_id='transfer_call_liza_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_transfer, 'token': token, 'chat_id': liza_chat_id, 'filepath': path_to_file_sql_airflow, 'filename': transfer_xlsx}, 
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

recall_edit_telegram = PythonOperator(
     task_id='recall_edit_telegram', 
     python_callable=telegram_send, 
     op_kwargs={'text': text_meetings, 'token': token, 'chat_id': chat_id, 'filepath': path_to_file_sql_airflow, 'filename': csv_recalls_edit1}, 
     dag=dag
     )

recall_edit_telegram1 = PythonOperator(
     task_id='recall_edit_telegram1', 
     python_callable=telegram_send, 
     op_kwargs={'text': text_meetings, 'token': token, 'chat_id': nastya_chat_id, 'filepath': path_to_file_sql_airflow, 'filename': csv_recalls_edit_xlsx}, 
     dag=dag
     )



[leads_sql,recalls_edit_sql] >> lids_upgrade >> [leads_telegram, leads_telegram1, leads_liza_telegram1, recall_edit_telegram, recall_edit_telegram1]
recalls_sql >>  recalls_telegram
transfers_sql >> transfers_telegram
request_sql >> meetings_telegram
transfer_call_sql >> transfer_edit >> transfer_call_telegram >> transfer_call_liza_telegram