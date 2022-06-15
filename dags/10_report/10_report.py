import pendulum
import paramiko
import shutil

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from scp import SCPClient
from smb.SMBConnection import SMBConnection
from commons.connect_db import connect_db
from commons.clear_file import clear_file


# Функция для перемещения файла с сервера MySQL на сервер Airflow.
# Необходимо передать абсолютный путь на обоих серверах, название файла, наименование сервера, откуда перемещаем файл.
def transfer_file(from_path, to_path, file, db):
    from time import sleep

    host, user, password = connect_db(db)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=user, password=password)

    sleep(10)

    scp = SCPClient(client.get_transport())

    # Откуда, куда.
    scp.get(f'{from_path}{file}', f'{to_path}{file}')
    scp.close()
    client.close()

    sleep(5)


def transfer_file_to_dbs(from_path, to_path, file, db):
    from time import sleep

    host, user, password = connect_db(db)
    conn = SMBConnection(username=user, password=password, my_name="Alexander Brezhnev", remote_name="samba", use_ntlm_v2=True)

    sleep(5)

    if conn.connect(host, 445):
        with open(f'{from_path}{file}', 'rb') as my_file:
            conn.storeFile('dbs', f'{to_path}{file}', my_file)
    conn.close()

    sleep(5)


# Функция для удаления файла с сервера.
# Сервер MySQL не позволяет записывать файл из SQL запроса, если такой файл уже существует.
# Необходимо передать абсолютный путь на сервере, название файла, наименование сервера, откуда удаляем файл.
def del_file(from_path, file, db):
    from time import sleep

    host, user, password = connect_db(db)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=user, password=password)

    # Откуда.
    stdin, stdout, stderr = client.exec_command(f'rm -f {from_path}{file}')

    sleep(5)


default_args = {
    'owner': 'Alexander Brezhnev',
    'mysql_conn_id': 'Maria_db',
    'start_date': pendulum.datetime(2022, 6, 9, tz='Europe/Kaliningrad'),
    'catchup': False
}

dag = DAG(
    dag_id='10_report',
    schedule_interval='50 3 * * *',
    default_args=default_args
)


path_to_file_airflow = '/root/airflow/dags/10_report/Files/'
path_to_file_mysql = '/home/glotov/84.201.164.249/10_report/'
path_to_file_dbs = '/10_report/'

all_users_del = PythonOperator(task_id='all_users_del', python_callable=del_file, 
op_kwargs={'from_path': path_to_file_mysql, 'file': 'All_users.csv', 'db': 'Server_MySQL'}, dag=dag)
super_del = PythonOperator(task_id='super_del', python_callable=del_file, 
op_kwargs={'from_path': path_to_file_mysql, 'file': 'Super.csv', 'db': 'Server_MySQL'}, dag=dag)
total_calls_del = PythonOperator(task_id='total_calls_del', python_callable=del_file, 
op_kwargs={'from_path': path_to_file_mysql, 'file': 'Total_calls.csv', 'db': 'Server_MySQL'}, dag=dag)
total_calls_31d_sql_del = PythonOperator(task_id='total_calls_31d_sql_del', python_callable=del_file, 
op_kwargs={'from_path': path_to_file_mysql, 'file': 'Total_calls_31d.csv', 'db': 'Server_MySQL'}, dag=dag)

all_users_sql = MySqlOperator(task_id='all_users_sql', sql='/SQL/All_users_to_csv.sql', dag=dag)
super_sql = MySqlOperator(task_id='super_sql', sql='/SQL/Super_to_csv.sql', dag=dag)
# total_calls_sql = MySqlOperator(task_id='total_calls_sql', sql='/SQL/Total_calls_to_csv.sql', dag=dag)
# total_calls_31d_sql = MySqlOperator(task_id='total_calls_31d_sql', sql='/SQL/Total_calls_31d_to_csv.sql', dag=dag)

all_users_transfer = PythonOperator(task_id='all_users_transfer', python_callable=transfer_file, 
op_kwargs={'from_path': path_to_file_mysql, 'to_path': path_to_file_airflow, 'file': 'All_users.csv', 'db': 'Server_MySQL'}, dag=dag)
super_transfer = PythonOperator(task_id='super_transfer', python_callable=transfer_file, 
op_kwargs={'from_path': path_to_file_mysql, 'to_path': path_to_file_airflow, 'file': 'Super.csv', 'db': 'Server_MySQL'}, dag=dag)
# total_calls_transfer = PythonOperator(task_id='total_calls_transfer', python_callable=transfer_file, 
# op_kwargs={'from_path': path_to_file_mysql, 'to_path': path_to_file_airflow, 'file': 'Total_calls.csv'}, dag=dag)
# total_calls_31d_sql_transfer = PythonOperator(task_id='total_calls_31d_sql_transfer', python_callable=transfer_file, 
# op_kwargs={'from_path': path_to_file_mysql, 'to_path': path_to_file_airflow, 'file': 'Total_calls_31d.csv'}, dag=dag)

all_users_clear = PythonOperator(task_id='all_users_clear', python_callable=clear_file, 
op_kwargs={'my_file': f'{path_to_file_airflow}All_users.csv'}, dag=dag)
super_clear = PythonOperator(task_id='super_clear', python_callable=clear_file, 
op_kwargs={'my_file': f'{path_to_file_airflow}Super.csv'}, dag=dag)

all_users_transfer_to_dbs = PythonOperator(task_id='all_users_transfer_to_dbs', python_callable=transfer_file_to_dbs, 
op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'All_users.csv', 'db': 'DBS'}, dag=dag)
super_transfer_to_dbs = PythonOperator(task_id='super_transfer_to_dbs', python_callable=transfer_file_to_dbs, 
op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'Super.csv', 'db': 'DBS'}, dag=dag)

all_users_del >> all_users_sql >> all_users_transfer >> all_users_clear >> all_users_transfer_to_dbs
super_del >> super_sql >> super_transfer >> super_clear >> super_transfer_to_dbs
# total_calls_del >> total_calls_sql >> total_calls_transfer
# total_calls_31d_sql_del >> total_calls_31d_sql >> total_calls_31d_sql_transfer
