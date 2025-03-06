from datetime import timedelta
import datetime
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from commons_liza.load_mysql import get_data


default_args = {
    'owner': 'Kunina ELizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG(
    dag_id='pokazateli_operatorov_dl',
    schedule_interval='0 4 * * *',
    start_date=pendulum.datetime(2023, 7, 7, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args,
    max_active_runs=1 
    )

# Количество дней, за который снимаем данные

numdays = 60

# Дата для выгрузки данных из бд

date_i = datetime.date.today() - datetime.timedelta(days=numdays)

# Данные для подключения к бд

# cloud_182 = ['base_dep_slave', 'IyHBh9mDBdpg', '192.168.1.182', 'suitecrm']
cloud_183 = ['base_dep_slave', 'IyHBh9mDBdpg', '192.168.1.183', 'suitecrm']

# Путь к sql файлам

sql_folder = '/root/airflow/dags/pokazateli_operatorov_dl/SQL/'

call_sql = f'{sql_folder}call.sql'
request_sql = f'{sql_folder}request.sql'
user_sql = f'{sql_folder}user.sql'
callwait_sql = f'{sql_folder}callwait.sql'


# Путь к файлам проекта

project_folder = '/root/airflow/dags/pokazateli_operatorov_dl/Files/'

call_csv = f'{project_folder}call.csv'
request_csv = f'{project_folder}request.csv'
user_csv = f'{project_folder}user.csv'
callwait_csv = f'{project_folder}callwait.csv'

# Путь к справочникам

city_csv = '/root/airflow/dags/commons_liza/dictionary/city.csv'

# Типы столбцов в таблицах

type_dict = { 
      'phone' : 'str', 
      'request_phone' : 'str',
      'queue_c' : 'str',
      'dialog' : 'str',
      'operator' : 'str',
      'supervisor' : 'str',
      'name' : 'str',
      'city' : 'str',
      'town' : 'str',
      'call_sec' : 'int',
      'short_call' : 'str',
      'request_status' : 'str',
      'team' : 'str',
      'user_id' : 'str',
      'call_status' : 'str'
    }


# Выгрузка данных из бд

download_call = PythonOperator(
    task_id='get_call', 
    python_callable=get_data, 
    op_kwargs={'sql_download' : call_sql, 
               'cloud': cloud_183, 
               'date_i' : date_i, 
               'file_path' : call_csv}, 
    dag=dag
    )

download_request = PythonOperator(
    task_id='get_request', 
    python_callable=get_data, 
    op_kwargs={'sql_download' : request_sql, 
               'cloud': cloud_183, 
               'date_i' : date_i, 
               'file_path' : request_csv}, 
    dag=dag
    )

download_user = PythonOperator(
    task_id='get_user', 
    python_callable=get_data, 
    op_kwargs={'sql_download' : user_sql, 
               'cloud': cloud_183, 
               'date_i' : date_i, 
               'file_path' : user_csv}, 
    dag=dag
    )

download_callwait = PythonOperator(
    task_id='get_callwait', 
    python_callable=get_data, 
    op_kwargs={'sql_download' : callwait_sql, 
               'cloud': cloud_183, 
               'date_i' : date_i, 
               'file_path' : callwait_csv}, 
    dag=dag
    )

















