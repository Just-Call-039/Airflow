import datetime
import dateutil.relativedelta
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv
from answering_expect.expect_editing import edit_ex
from commons_li.clear_folder import clear_folder
from commons_li.sql_query_semicolon_to_csv import sql_query_to_csv_sc
from answering_expect.load_backup import load_expect
 

default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='expect_of_autootvet',
    schedule_interval='15 5 * * *',
    start_date=pendulum.datetime(2023, 7, 13, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


# cloud_name = 'cloud_128'
cloud_name = 'cloud_183'

path_to_file_airflow = '/root/airflow/dags/answering_expect/Files/'

path_dbs_total = '/Отчеты BI/Автоответчики/'

sql = '/root/airflow/dags/answering_expect/SQL/expect_auto.sql'

n = 1

today = datetime.date.today()
previous_date = today - datetime.timedelta(days=n)
year = previous_date.year
month = previous_date.month
day = previous_date.day

file_expect = f'Экспекты {day}_{month}_{year}.csv' 
file_sql = 'exp.csv' 



expect_sql = PythonOperator(
    task_id='expect_sql', 
    python_callable=load_expect, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql, 'path_csv_file': path_to_file_airflow, 'name_csv_file': file_sql, 'n' : n}, 
    dag=dag
    )

edit_of_autootvet_ex = PythonOperator(
    task_id='edit_of_autootvet_ex', 
    python_callable=edit_ex, 
    op_kwargs={'path_file': path_to_file_airflow, 'expect': file_sql, 'n' : n, 'total' : file_expect}, 
    dag=dag
    )

# ex_to_dbs = PythonOperator(
#     task_id='ex_to_dbs', 
#     python_callable=transfer_file_to_dbs, 
#     op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_dbs_total, 'file': file_expect, 'db': 'DBS'}, 
#     dag=dag
# )

clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder, 
    op_kwargs={'folder': path_to_file_airflow}, 
    dag=dag
    )


expect_sql >> edit_of_autootvet_ex >> clear_folders