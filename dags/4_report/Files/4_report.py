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
from commons.transfer_file import transfer_file
from commons.transfer_file_to_dbs import transfer_file_to_dbs
from commons.del_file import del_file


default_args = {
    'owner': 'Alexander Brezhnev',
    'mysql_conn_id': 'Maria_db',
    'start_date': pendulum.datetime(2022, 8, 7, tz='Europe/Kaliningrad'),
    'catchup': False
}

dag = DAG(
    dag_id='4_report',
    schedule_interval='0 9,11,13,15 * * *',
    default_args=default_args
)
