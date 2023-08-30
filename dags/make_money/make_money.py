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
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
    }


dag = DAG(
    dag_id='make_money',
    schedule_interval='5 5 * * *',
    start_date=pendulum.datetime(2023, 8, 28, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud_name = 'cloud_128'

token = '5095756650:AAElXGJb5kfvanEXx5FlET6T3HayTjIs_PU'
chat_id = '-1001412983860'  # your chat id

today_date = datetime.now().strftime("%d/%m/%Y")
text_money = today_date+' Отправляем файл make money'

path_to_sql = '/root/airflow/dags/make_money/SQL/make_money_sql_query.sql'

path_to_file_sql_airflow = '/root/airflow/dags/make_money/Files'

csv_money = 'make_money_file.csv'

# Блок выполнения SQL запросов.
money_sql = PythonOperator(
    task_id='money_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': path_to_sql, 'path_csv_file': path_to_file_sql_airflow, 'name_csv_file': csv_money}, 
    dag=dag
    )

money_telegram = PythonOperator(
    task_id='money_telegram', 
    python_callable=telegram_send, 
    op_kwargs={'text': text_money, 'token': token, 'chat_id': chat_id, 'filepath': path_to_file_sql_airflow, 'filename': csv_money}, 
    dag=dag
    )

money_sql >> money_telegram
