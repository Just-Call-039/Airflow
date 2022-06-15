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

default_args = {
    'owner': 'Alexander Brezhnev',
    'mysql_conn_id': 'Maria_db',
    'start_date': pendulum.datetime(2022, 6, 15, tz='Europe/Kaliningrad'),
    'catchup': False
}

dag = DAG(
    dag_id='test_dag',
    schedule_interval='50 3 * * *',
    default_args=default_args
)


def test():
    conn = SMBConnection(username="dbs01", password="S@LeS*41011", my_name="Alexander Brezhnev", remote_name="samba", use_ntlm_v2=True)
    smb_file = '/10_report/All_users.csv'
    if conn.connect("10.88.22.128", 445):
        print(conn.listShares())
        with open('/root/airflow/dags/10_report/Files/All_users.csv', 'rb') as f:
            conn.storeFile('dbs', smb_file, f)

dbs_transfer = PythonOperator(task_id='smb', python_callable=test, dag=dag)
