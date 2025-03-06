import pendulum
from datetime import timedelta
from datetime import date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from route_robotlogs.clear_folder import clear_folder

from route_robotlogs.save_to_dbs import save_to_dbs
from trunk.get_data import get_call, get_trunk

default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='trunk_per_day',
    schedule_interval='0 21 * * *',
    start_date=pendulum.datetime(2023, 7, 25, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

i_date = date.today()


call_name = 'call_{y}_{m}_{d}.csv'.format(y = i_date.year, m = '{0:0>2}'.format(i_date.month), d = '{0:0>2}'.format(i_date.day))
trunk_name = 'trunk_{y}_{m}_{d}.csv'.format(y = i_date.year, m = '{0:0>2}'.format(i_date.month), d = '{0:0>2}'.format(i_date.day))

file_project = '/root/airflow/dags/trunk/Files/'
call_path = f'{file_project}call/'
trunk_path = f'{file_project}trunk/'

call_sql = '/root/airflow/dags/trunk/SQL/call.sql'
trunk_sql = '/root/airflow/dags/trunk/SQL/trunk.sql'

call_dbs_path = '/scripts fsp/Current Files/trunk/per_day/call/'
trunk_dbs_path = '/scripts fsp/Current Files/trunk/per_day/trunk/'


get_calls = PythonOperator(
    task_id = 'get_call',
    python_callable = get_call,
    op_kwargs = {'sql_path' : call_sql,
                'file_path' : call_path,
                'file_name' : call_name},
    dag=dag
    )

get_trunks  = PythonOperator(
    task_id = 'get_trunk',
    python_callable = get_trunk,
    op_kwargs = {'sql_path' : trunk_sql,
                'file_path' : trunk_path,
                'file_name' : trunk_name},
    dag=dag
    )

save_call = PythonOperator(
    task_id = 'save_call',
    python_callable = save_to_dbs,
    op_kwargs = {'path_from' : call_path,
                 'path_to' : call_dbs_path},
    dag=dag
    )

save_trunk = PythonOperator(
    task_id = 'save_trunk',
    python_callable = save_to_dbs,
    op_kwargs = {'path_from' : trunk_path,
                 'path_to' : trunk_dbs_path},
    dag=dag
    )

clear_folders = PythonOperator(
    task_id = 'clear_folders', 
    python_callable = clear_folder, 
    op_kwargs = {'folder': file_project}, 
    dag = dag
    )

get_calls >> save_call
get_trunks >> save_trunk 
[save_call, save_trunk] >> clear_folders

