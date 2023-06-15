from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.repeat_download import repeat_download
from fsp.repeat_download import sql_query_to_csv


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

n = 1
days = 1
cloud_name = 'cloud_128'

path_to_sql = '/root/airflow/dags/dispetchers_4_50/SQL/'
sql_leads = f'{path_to_sql}Leads_4_50.sql'
sql_recalls = f'{path_to_sql}Recalls_4_50.sql'

path_to_file_sql_airflow = '/root/airflow/dags/dispetchers_4_50/Files/'

csv_leads = 'leads_4_50.csv'
csv_recalls = 'recalls_4_50.csv'

# Блок выполнения SQL запросов.
leads_sql = PythonOperator(
    task_id='leads_sql', 
    python_callable=repeat_download, 
    op_kwargs={'n': n, 'days': days, 'cloud': cloud_name, 'path_sql_file': path_to_sql, 'path_csv_file': sql_leads, 'name_csv_file': csv_leads, 'source': ''}, 
    dag=dag
    )

recalls_sql = PythonOperator(
    task_id='recalls_sql', 
    python_callable=repeat_download, 
    op_kwargs={'n': n, 'days': days, 'cloud': cloud_name, 'path_sql_file': path_to_sql, 'path_csv_file': sql_recalls, 'name_csv_file': csv_recalls, 'source': ''}, 
    dag=dag
    )

recalls_sql
leads_sql