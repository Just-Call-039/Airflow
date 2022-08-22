from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator
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

# Блок выполнения SQL запросов.
main_sql = MySqlOperator(
    task_id='main_sql', 
    sql='/SQL/Main_to_csv.sql', 
    dag=dag
    )
working_time_sql = MySqlOperator(
    task_id='working_time_sql', 
    sql='/SQL/Working_time_to_csv.sql', 
    dag=dag
    )
users_total_sql = MySqlOperator(
    task_id='users_total_sql', 
    sql='/SQL/Users_total_to_csv.sql', 
    dag=dag
    )

# Блок перемещения файлов с сервера MySQL на сервер Airflow.
main_transfer = PythonOperator(
    task_id='main_transfer', 
    python_callable=transfer_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'to_path': path_to_file_airflow, 'file': 'Main.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
working_time_transfer = PythonOperator(
    task_id='working_time_transfer', 
    python_callable=transfer_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'to_path': path_to_file_airflow, 'file': 'Working_time.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
users_total_transfer = PythonOperator(
    task_id='users_total_transfer', 
    python_callable=transfer_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'to_path': path_to_file_airflow, 'file': 'Users_total.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )

# Блок отправки всех файлов в папку DBS.
main_transfer_to_dbs = PythonOperator(
    task_id='main_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'Main.csv', 'db': 'DBS'}, 
    dag=dag
    )
working_time_transfer_to_dbs = PythonOperator(
    task_id='working_time_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'Working_time.csv', 'db': 'DBS'}, 
    dag=dag
    )
users_total_transfer_to_dbs = PythonOperator(
    task_id='users_total_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'Users_total.csv', 'db': 'DBS'}, 
    dag=dag
    )

# Блок очередности выполнения задач.
main_del >> main_sql >> main_transfer >> main_transfer_to_dbs
working_time_del >> working_time_sql >> working_time_transfer >> working_time_transfer_to_dbs
users_total_del >> users_total_sql >> users_total_transfer >> users_total_transfer_to_dbs
