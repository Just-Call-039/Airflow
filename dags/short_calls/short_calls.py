import datetime
import dateutil.relativedelta
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv
from short_calls.calls_editers import short_editer
from commons_li.clear_folder import clear_folder
from commons_li.sql_query_semicolon_to_csv import sql_query_to_csv_sc


default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

def print_file_contents(file_path):
    with open(file_path, 'r') as file:
        file_contents = file.read()
        print(file_contents)

dag = DAG(
    dag_id='short_calls',
    schedule_interval='55 6 * * *',
    start_date=pendulum.datetime(2023, 9, 30, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

path_to_file_airflow = '/root/airflow/dags/short_calls/Files/'
path_airflow_sql = f'{path_to_file_airflow}sql_files/'
path_airflow_total = f'{path_to_file_airflow}total_file/'

path_dbs_total = '/short calls/Звонки/'
path_dbs_emissions = '/scripts fsp/Current Files/'

sql_calls = '/root/airflow/dags/short_calls/SQL/calls_short.sql'
sql_out_calls = '/root/airflow/dags/short_calls/SQL/out_short_calls.sql'
sql_robot = '/root/airflow/dags/short_calls/SQL/robot_short.sql'
sql_emissions = '/root/airflow/dags/short_calls/SQL/emissions.sql'


today = datetime.date.today()
previous_date = today - datetime.timedelta(days=1)
year = previous_date.year
month = previous_date.month
day = previous_date.day

file_total = f'transfers {day}_{month}_{year}.csv' 
calls_csv = 'calls.csv'
calls_out_csv = 'calls_out.csv'
robot_csv = 'robot.csv'
emissions_csv = 'emissions.csv'

file_path = '/root/airflow/dags/not_share/cloud_my_sql_182.csv'




print_task = PythonOperator(
    task_id='print_file_contents_task',
    python_callable=print_file_contents,
    op_args=[file_path],
    dag=dag,
)


# Блок выполнения SQL запросов.
calls_sql = PythonOperator(
    task_id='calls_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_calls, 'path_csv_file': path_airflow_sql, 'name_csv_file': calls_csv}, 
    dag=dag
    )
calls_out_sql = PythonOperator(
    task_id='calls_out_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_out_calls, 'path_csv_file': path_airflow_sql, 'name_csv_file': calls_out_csv}, 
    dag=dag
    )
robot_sql = PythonOperator(
    task_id='robot_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_robot, 'path_csv_file': path_airflow_sql, 'name_csv_file': robot_csv}, 
    dag=dag
    )
emissions_sql = PythonOperator(
    task_id='emissions_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_emissions, 'path_csv_file': path_to_file_airflow, 'name_csv_file': emissions_csv}, 
    dag=dag
    )

# Преобразование файлов после sql.
short_calls_editing = PythonOperator(
    task_id='short_calls_editing', 
    python_callable=short_editer, 
    op_kwargs={'path_to_files': path_airflow_sql,'calls': calls_csv,'calls_out': calls_out_csv,'robot': robot_csv, 
               'path_result': path_airflow_total, 'file_result': file_total}, 
    dag=dag
    )

# Отправка файла на сервер dbs

transfer_to_dbs = PythonOperator(
    task_id='transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_total, 'to_path': path_dbs_total, 'file': file_total, 'db': 'DBS'}, 
    dag=dag
)

transfer_emissions_to_dbs = PythonOperator(
    task_id='transfer_emissions_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_dbs_emissions, 'file': emissions_csv, 'db': 'DBS'}, 
    dag=dag
)
# Очистка папки

clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder, 
    op_kwargs={'folder': path_airflow_total}, 
    dag=dag
    )

print_task >> [calls_sql, calls_out_sql, robot_sql] >> short_calls_editing >> transfer_to_dbs >> clear_folders
emissions_sql >> transfer_emissions_to_dbs


