from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.repeat_download import sql_query_to_csv2
from fsp.repeat_download import sql_query_to_csv
from report_25_today.calls_editer import robotlog_calls_transformation
from report_25_today.calls_to_clickhouse import calls_to_clickhouse


default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='25_report_today',
    schedule_interval='18 6-22 * * *',
    start_date=pendulum.datetime(2022, 11, 22, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

path_to_sql = '/root/airflow/dags/report_25_today/SQL/'
sql_total_calls_last_hour = f'{path_to_sql}Total_calls_last_hour.sql'
sql_transfer_steps = f'{path_to_sql}Transfer_steps.sql'
sql_steps = f'{path_to_sql}steps.sql'
sql_transfers = f'{path_to_sql}transfers.sql' 

path_to_file_sql_rl_airflow = '/root/airflow/dags/report_25_today/Files/sql_robot_log/'
path_to_file_airflow = '/root/airflow/dags/report_25_today/Files/'
path_to_calls = '/root/airflow/dags/report_25_today/Files/robot_log/'

csv_calls = 'calls_last_hour.csv'
csv_transfer_steps = 'transfer_steps.csv'
csv_steps = 'steps.csv'
csv_transfers = 'transfers.csv'
csv_city = 'city.csv'
csv_town = 'town.csv'
csv_region = 'region.csv'


# Блок выполнения SQL запросов.
calls_last_hour_sql = PythonOperator(
    task_id='calls_last_hour_sql', 
    python_callable=sql_query_to_csv2, 
    op_kwargs={'cloud': cloud_name, 'db': "suitecrm_robot", 'path_sql_file': sql_total_calls_last_hour, 'path_csv_file': path_to_file_sql_rl_airflow, 'name_csv_file': csv_calls}, 
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
    op_kwargs={'path_to_sql_calls': path_to_calls, 'calls': csv_calls}, 
    dag=dag
    )

# Очередности выполнения задач.
[calls_last_hour_sql,
transfer_steps_sql,
transfers_sql,
steps_sql] >> transformation_calls >> calls_file_to_clickhouse