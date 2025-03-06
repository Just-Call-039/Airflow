from datetime import timedelta
from datetime import datetime
# import datetime
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.repeat_download import sql_query_to_csv
from commons_sawa.telegram import telegram_send
from commons_li.clear_folder import clear_folder
from make_money.domru_edit import lids_editing

default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
    }


dag = DAG(
    dag_id='domru_ohvat',
    schedule_interval='0 8 11 * *',
    start_date=pendulum.datetime(2024, 4, 28, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

# cloud_name = 'cloud_128'
cloud_name = 'cloud_183'

token = '6583071346:AAFEMd72_v33VEy90SxVcZ8Bzp3t4GZ0OJE'
chat_id = '-1002108285575'  # your chat id

today_date = datetime.now().strftime("%d/%m/%Y")
text_money = today_date +' Отправляем файл по охватам домру'

import datetime

today = datetime.date.today()
year = today.year % 100
month = today.month

url = f'http://rias-web.rias.r-one.io/files/partners-10-{month:02}-{year:02}.csv'

path_to_file_sql_airflow = '/root/airflow/dags/make_money/Files/'

csv_domru = f'Домру техохваты {today}.csv'



edit_file = PythonOperator(
    task_id='edit_file', 
    python_callable=lids_editing, 
    op_kwargs={'url': url,'path_to_file_sql_airflow': path_to_file_sql_airflow,'csv_domru': csv_domru}, 
    dag=dag
    )

money_telegram = PythonOperator(
    task_id='money_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_money, 'token': token, 'chat_id': chat_id, 'filepath': path_to_file_sql_airflow, 'filename': csv_domru}, 
    dag=dag
    )


clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder, 
    op_kwargs={'folder': path_to_file_sql_airflow}, 
    dag=dag
    )

edit_file >> money_telegram 
