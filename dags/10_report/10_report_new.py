from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from commons.clear_file import clear_file
from commons.transfer_file import transfer_file
from commons.transfer_file_to_dbs import transfer_file_to_dbs
from commons.del_file import del_file
from commons.sql_query_to_csv import sql_query_to_csv


default_args = {
    'owner': 'Alexander Brezhnev',
    'email': 'brezhnev.aleksandr@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'mysql_conn_id': 'cloud_my_sql_117',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='10_report_new',
    schedule_interval='50 5-13/4 * * *',
    start_date=pendulum.datetime(2022, 6, 16, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


path_to_file_airflow = '/root/airflow/dags/10_report/Files/'
path_to_file_mysql = '/home/glotov/192.168.1.117/10_report/'
path_to_file_dbs = '/10_report/Files/'
path_to_file_dbs_4_rep = '/4_report/Files/'

cloud_name = 'cloud_128'
sql_folder = '/root/airflow/dags/10_report/SQL/'
sql_all_users = f'{sql_folder}All_users.sql'
sql_super = f'{sql_folder}Super.sql'

# Блок выполнения SQL запросов.
all_users_sql = PythonOperator(
    task_id='all_users_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_all_users, 'path_csv_file': path_to_file_airflow, 'name_csv_file': 'All_users.csv'}, 
    dag=dag
    )
super_sql = PythonOperator(
    task_id='super_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_super, 'path_csv_file': path_to_file_airflow, 'name_csv_file': 'Super.csv'}, 
    dag=dag
    )

# Блок преобразования пользователей и супервайзеров.
all_users_clear = PythonOperator(
    task_id='all_users_clear', 
    python_callable=clear_file, 
    op_kwargs={'my_file': f'{path_to_file_airflow}All_users.csv'}, 
    dag=dag
    )
super_clear = PythonOperator(
    task_id='super_clear', 
    python_callable=clear_file, 
    op_kwargs={'my_file': f'{path_to_file_airflow}Super.csv'}, 
    dag=dag
    )

# Блок отправки всех файлов в папку DBS.
all_users_transfer_to_dbs = PythonOperator(
    task_id='all_users_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'All_users.csv', 'db': 'DBS'}, 
    dag=dag
    )
all_users_clear_transfer_to_dbs = PythonOperator(
    task_id='all_users_clear_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'All_users_clear.csv', 'db': 'DBS'}, 
    dag=dag
    )
super_transfer_to_dbs = PythonOperator(
    task_id='super_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'Super.csv', 'db': 'DBS'}, 
    dag=dag
    )
super_clear_transfer_to_dbs = PythonOperator(
    task_id='super_clear_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'Super_clear.csv', 'db': 'DBS'}, 
    dag=dag
    )

# Два файла нужны для отчета №4.
all_users_clear_transfer_to_dbs_4_rep = PythonOperator(
    task_id='all_users_clear_transfer_to_dbs_4_rep', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs_4_rep, 'file': 'All_users_clear.csv', 'db': 'DBS'}, 
    dag=dag
    )
super_clear_transfer_to_dbs_4_rep = PythonOperator(
    task_id='super_clear_transfer_to_dbs_4_rep', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs_4_rep, 'file': 'Super_clear.csv', 'db': 'DBS'}, 
    dag=dag
    )

all_users_sql >> all_users_clear >> [all_users_transfer_to_dbs, all_users_clear_transfer_to_dbs, all_users_clear_transfer_to_dbs_4_rep]
super_sql >> super_clear >> [super_transfer_to_dbs, super_clear_transfer_to_dbs, super_clear_transfer_to_dbs_4_rep]
