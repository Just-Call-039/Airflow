from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.repeat_download import repeat_download
from fsp.repeat_download import sql_query_to_csv
from report_25_last_week.calls_editer import robotlog_calls_transformation
from report_25_last_week.calls_to_clickhouse import calls_to_clickhouse


default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='25_report_last_week',
    schedule_interval='0 7 * * *',
    start_date=pendulum.datetime(2022, 11, 22, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

n = 1
days = 7
cloud_name = 'cloud_128'

path_to_sql = '/root/airflow/dags/report_25_last_week/SQL/'
sql_total_calls_last_week = f'{path_to_sql}Total_calls_last_week.sql'
sql_transfer_steps = f'{path_to_sql}Transfer_steps.sql'
sql_steps = f'{path_to_sql}steps.sql'
sql_transfers = f'{path_to_sql}transfers.sql' 

path_to_file_sql_rl_airflow = '/root/airflow/dags/report_25_last_week/Files/sql_robot_log/'
path_to_file_airflow = '/root/airflow/dags/report_25_last_week/Files/'
path_to_calls = '/root/airflow/dags/report_25_last_week/Files/robot_log/'

csv_calls = 'calls_last_week_{}.csv'
csv_transfer_steps = 'transfer_steps.csv'
csv_steps = 'steps.csv'
csv_transfers = 'transfers.csv'
csv_city = 'city.csv'
csv_town = 'town.csv'
csv_region = 'region.csv'


# Блок выполнения SQL запросов.
calls_last_week_sql = PythonOperator(
    task_id='calls_last_week_sql', 
    python_callable=repeat_download, 
    op_kwargs={'n': n, 'days': days, 'cloud': cloud_name, 'path_sql_file': sql_total_calls_last_week, 'path_csv_file': path_to_file_sql_rl_airflow, 'name_csv_file': csv_calls, 'source': ''}, 
    dag=dag
    )


transfer_steps_sql = PythonOperator(
    task_id='transfer_steps_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_transfer_steps, 'path_csv_file': path_to_file_airflow, 'name_csv_file': csv_transfer_steps}, 
    dag=dag
    )

steps_sql = PythonOperator(
    task_id='steps_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_steps, 'path_csv_file': path_to_file_airflow, 'name_csv_file': csv_steps}, 
    dag=dag
    )

transfers_sql = PythonOperator(
    task_id='transfers_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_transfers, 'path_csv_file': path_to_file_airflow, 'name_csv_file': csv_transfers}, 
    dag=dag
    )

# Преобразование файлов после sql.
transformation_calls = PythonOperator(
    task_id='calls_transformation', 
    python_callable=robotlog_calls_transformation, 
    op_kwargs={'path_to_sql_calls': path_to_file_sql_rl_airflow, 'sql_calls': csv_calls, 'path_to_sql_transfer_steps': path_to_file_airflow,
                'sql_transfer_steps': csv_transfer_steps, 'sql_steps': csv_steps, 'sql_transfers': csv_transfers, 'path_to_calls': path_to_calls,
                'sql_city': csv_city, 'sql_town': csv_town, 'sql_region': csv_region}, 
    dag=dag
    )

# Перенос файла в кликхаус.
calls_file_to_clickhouse = PythonOperator(
    task_id='calls_file_to_clickhouse', 
    python_callable=calls_to_clickhouse, 
    op_kwargs={'path_to_sql_calls': path_to_calls, 'csv_calls': csv_calls}, 
    dag=dag
    )

# Очередности выполнения задач.
[calls_last_week_sql,
transfer_steps_sql,
transfers_sql,
steps_sql] >> transformation_calls >> calls_file_to_clickhouse