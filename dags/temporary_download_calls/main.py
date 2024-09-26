import datetime
import dateutil.relativedelta
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from fsp.repeat_download import sql_query_to_csv



default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='temporary_download_calls',
    schedule_interval='20 5 1 * *',
    start_date=pendulum.datetime(2023, 9, 30, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

path_airflow_c = '/root/airflow/dags/indicators_to_regions/Files/sql_files/callls/'

# path_dbs_c = '/Отчеты BI/Показатели до регионов/Для архива/'


sql_main_c = '/root/airflow/dags/temporary_download_calls/call.sql'

year = '2024'
month = '06'
file_c = f'calls {month}_{year}.csv'


c_sql = PythonOperator(
    task_id='c_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main_c, 'path_csv_file': path_airflow_c, 'name_csv_file': file_c}, 
    dag=dag
    )

# c_to_dbs = PythonOperator(
#     task_id='c_to_dbs', 
#     python_callable=transfer_file_to_dbs, 
#     op_kwargs={'from_path': path_airflow_c, 'to_path': path_dbs_c, 'file': file_c, 'db': 'DBS'}, 
#     dag=dag
#     )
c_sql
# c_sql >> c_to_dbs