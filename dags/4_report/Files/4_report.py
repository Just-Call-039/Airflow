from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from commons.clear_file import clear_file
from commons.transfer_file import transfer_file
from commons.transfer_file_to_dbs import transfer_file_to_dbs
from commons.del_file import del_file


default_args = {
    'owner': 'Alexander Brezhnev',
    'email': 'brezhnev.aleksandr@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'mysql_conn_id': 'Maria_db',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.datetime(2022, 8, 7, tz='Europe/Kaliningrad'),
    'catchup': False
}

dag = DAG(
    dag_id='4_report',
    schedule_interval='0 9-15/2 * * *',
    default_args=default_args
)


path_to_file_airflow = '/root/airflow/dags/4_report/Files/'
path_to_file_mysql = '/home/glotov/84.201.164.249/4_report/'
path_to_file_dbs = '/4_report/Files/'

# Блок предварительного удаления файлов с сервера.
main_del = PythonOperator(
    task_id='main_del', 
    python_callable=del_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'file': 'Main.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
working_time_del = PythonOperator(
    task_id='working_time_del', 
    python_callable=del_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'file': 'Working_time.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
users_total_del = PythonOperator(
    task_id='users_total_del', 
    python_callable=del_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'file': 'Users_total.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
