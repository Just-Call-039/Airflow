import datetime
import dateutil.relativedelta
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv
from incoming_traffic.traffic_editer import traffic_in_editing
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
    dag_id='traffic_in_line',
    schedule_interval='0 7 * * *',
    start_date=pendulum.datetime(2024, 1, 31, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

path_to_file_airflow = '/root/airflow/dags/incoming_traffic/Files/'
path_airflow_sql = f'{path_to_file_airflow}sql_files/'
path_airflow_total = f'{path_to_file_airflow}total_file/'

path_dbs_calls = '/Отчеты BI/Входящая линия/Трафик входа категории/Журнал/'
path_dbs_lids = '/Отчеты BI/Входящая линия/Трафик входа категории/Лиды/'
path_dbs_traffic_all = '/Отчеты BI/Входящая линия/Трафик входа категории/Обрывы и перезвоны/'
path_dbs_traffic = '/Отчеты BI/Входящая линия/Трафик входа категории/Трафик/'

sql_calls = '/root/airflow/dags/incoming_traffic/SQL/звонки 4-12.sql'
sql_robot = '/root/airflow/dags/incoming_traffic/SQL/robot.sql'
sql_planned = '/root/airflow/dags/incoming_traffic/SQL/planned.sql'
sql_ishod = '/root/airflow/dags/incoming_traffic/SQL/ishod.sql'
sql_trafic_in= '/root/airflow/dags/incoming_traffic/SQL/trafic_in.sql'
sql_trafic_all = '/root/airflow/dags/incoming_traffic/SQL/Перезвоны и обрывы.sql'


today = datetime.date.today()
year = today.year
month = today.month
file_calls = f'Журнал операторы_{year}_{month:02}.csv'
file_lids = f'Лиды_{year}_{month:02}.csv'
file_traffic = f'Трафик_{year}_{month:02}.csv'
file_traffic_all = f'Трафик_журнала_{year}_{month:02}.csv'
calls_csv = 'calls.csv'
robot_csv = 'robot.csv'
plancall_csv = 'plancall.csv'
ishod_csv = 'ishod.csv'
trafic_csv = 'trafic.csv'


# Блок выполнения файлов sql 
calls_sql = PythonOperator(
    task_id='calls_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_calls, 'path_csv_file': path_airflow_sql, 'name_csv_file': calls_csv}, 
    dag=dag
    )
robot_sql = PythonOperator(
    task_id='robot_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_robot, 'path_csv_file': path_airflow_sql, 'name_csv_file': robot_csv}, 
    dag=dag
    )
plancall_sql = PythonOperator(
    task_id='plancall_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_planned, 'path_csv_file': path_airflow_sql, 'name_csv_file': plancall_csv}, 
    dag=dag
    )
ishod_sql = PythonOperator(
    task_id='ishod_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_ishod, 'path_csv_file': path_airflow_sql, 'name_csv_file': ishod_csv}, 
    dag=dag
    )
trafic_sql = PythonOperator(
    task_id='trafic_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_trafic_in, 'path_csv_file': path_airflow_sql, 'name_csv_file': trafic_csv}, 
    dag=dag
    )
traffic_all_sql = PythonOperator(
    task_id='traffic_all_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_trafic_all, 'path_csv_file': path_airflow_total, 'name_csv_file': file_traffic_all}, 
    dag=dag
    )


# Преобразование файлов после sql.
inbound_calls_editing = PythonOperator(
    task_id='inbound_calls_editing', 
    python_callable=traffic_in_editing, 
    op_kwargs={'calls': calls_csv, 'robot': robot_csv, 'plancall': plancall_csv, 
               'ishod': ishod_csv,'trafic': trafic_csv, 'path_file': path_airflow_sql, 'calls_file': file_calls,
            'traffic_file': file_traffic,'path_to_file': path_airflow_total, 'lids_file': file_lids}, 
    dag=dag
    )



# Отправка файлов на сервер dbs
calls_to_dbs = PythonOperator(
    task_id='calls_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_total, 'to_path': path_dbs_calls, 'file': file_calls, 'db': 'DBS'}, 
    dag=dag
    )

traffic_all_to_dbs = PythonOperator(
    task_id='traffic_all_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_total, 'to_path': path_dbs_traffic_all, 'file': file_traffic_all, 'db': 'DBS'}, 
    dag=dag
    )

lids_to_dbs = PythonOperator(
    task_id='lids_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_total, 'to_path': path_dbs_lids, 'file': file_lids, 'db': 'DBS'}, 
    dag=dag
    )

traffic_to_dbs = PythonOperator(
    task_id='traffic_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_total, 'to_path': path_dbs_traffic, 'file': file_traffic, 'db': 'DBS'}, 
    dag=dag
    )




[calls_sql, robot_sql, plancall_sql, ishod_sql, trafic_sql] >> inbound_calls_editing >> [calls_to_dbs,lids_to_dbs, traffic_to_dbs]
traffic_all_sql >> traffic_all_to_dbs