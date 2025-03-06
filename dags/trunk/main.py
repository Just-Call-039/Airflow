import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from route_robotlogs.save_to_dbs import save_to_dbs
from trunk.get_data import get_call

default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='trunk',
    schedule_interval='0,5,10,15,20,25,30,35,40,45,50,55 7-19 * * *',
    start_date=pendulum.datetime(2023, 7, 25, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

file_name = 'call.csv'
file_path = '/root/airflow/dags/trunk/Files/operational/'
sql_path = '/root/airflow/dags/trunk/SQL/call.sql'
dbs_path = '/scripts fsp/Current Files/trunk/'

get_data = PythonOperator(
    task_id = 'get_call',
    python_callable = get_call,
    op_kwargs = {'sql_path' : sql_path,
                'file_path' : file_path,
                'file_name' : file_name},
    dag=dag
    )

save_data = PythonOperator(
    task_id = 'save_call',
    python_callable = save_to_dbs,
    op_kwargs = {'path_from' :  file_path,
                'path_to' : dbs_path},
    dag=dag
    )

get_data >> save_data

