import datetime
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from commons_liza.clear_folder import clear_folder
from gas.proccess import all_proccess, sent_to_dbs


default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='gas',
    schedule_interval='30 21 * * *',
    start_date=pendulum.datetime(2025, 2, 1, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

# Определяем дату за которую будем выгружать данные
n = 0  # Количество дней назад от текущей даты
date_i = datetime.date.today() - datetime.timedelta(days=n)
year = date_i.year
month = date_i.month
day = date_i.day

# Данные для подклбчения

cloud_182 = ['base_dep_slave', 'IyHBh9mDBdpg', '192.168.1.182', 'suitecrm'] 
cloud_42 = ['gas_manager', 'teBxoh-2dekpy-ruvrid', '192.168.1.42', 'gasification'] 

# Пути к sql запросам

contact_sql = '/root/airflow/dags/gas/SQL/contacts.sql'
gas_sql = '/root/airflow/dags/gas/SQL/gas.sql'
robotlog_sql = '/root/airflow/dags/gas/SQL/robotlog.sql'
sms_sql = '/root/airflow/dags/gas/SQL/sms.sql'
shedex_sql = '/root/airflow/dags/gas/SQL/shedex.sql'
brigada_sql = '/root/airflow/dags/gas/SQL/brigada.sql'

# Путь к файлам проекта

folder_path = '/root/airflow/dags/gas/Files/'

# Названия для файлов

contact_csv = f'{folder_path}contact_{year:02}_{month:02}_{day:02}.csv'
gas_csv = f'{folder_path}gas_{year:02}_{month:02}_{day:02}.csv'
robotlog_csv = f'{folder_path}robotlog_{year:02}_{month:02}_{day:02}.csv'
sms_csv = f'{folder_path}sms_{year:02}_{month:02}_{day:02}.csv'
shedex_csv = f'{folder_path}shedex_{year:02}_{month:02}_{day:02}.csv'
brigada_csv = f'{folder_path}brigada.csv'


# Справочник с типами данных

type_dict = {'phone' : 'str', 
             'last_queue' : 'str', 
             'last_step' : 'str', 
             'contact_status' : 'str',
             'campaign_name' : 'str',
             'contract' : 'str', 
             'territory' : 'str', 
             'address' : 'str', 
             'flat' : 'str', 
             'brigade' : 'str',
             'territory_name' : 'str', 
             'id_client_address' : 'str', 
             'service' : 'str',
             'queue' : 'str', 
             'inbound' : 'str',
             'sms_status' : 'str',
             'message_text' : 'str',
             'id_address' : 'str',
             'request_status' : 'str',
             'shedex_id' : 'str'}


# Путь к папке dbs

dbs_path = 'scripts fsp\Current Files\gas'

# Функция, в которой загружаются с базы данных все файлы и сохраняются в csv. Манипуляции над ними минимальные. 
# Передаем в функцию:
#  пути к файлам sql (название переменных *_sql) для выгрузки данных
#  пути к файлам csv (названия переменных *_csv)
#  списки с данными для доступа в бд (cloud_*)
#  date_i дата, за которую выгрузаем данные
#  type_dict - словарь, в котором собранны все типы данных для столбцов во всех таблицах


procceess = PythonOperator(
    task_id = 'all_proccess',
    python_callable = all_proccess,
    op_kwargs = {'cloud_183' : cloud_182, 
                 'cloud_42' : cloud_42, 
                 'date_i' : date_i, 
                 'type_dict' : type_dict, 
                 'contact_sql' : contact_sql, 
                 'contact_csv' : contact_csv, 
                 'gas_sql' : gas_sql,
                 'gas_csv' : gas_csv, 
                 'robotlog_sql' : robotlog_sql, 
                 'robotlog_csv' : robotlog_csv, 
                 'sms_sql' : sms_sql, 
                 'sms_csv' : sms_csv, 
                 'shedex_sql' : shedex_sql, 
                 'shedex_csv' : shedex_csv, 
                 'brigada_sql' : brigada_sql, 
                 'brigada_csv' : brigada_csv
                },
    dag = dag
    ) 

# Функция для отправки полученных таблиц на dbs
# Передаем в функцию путь к папке проекта на airflow со всеми файлами и путь к папке проекта на dbs

save_files_dbs = PythonOperator(
    task_id = 'save_files_dbs',
    python_callable = sent_to_dbs,
    op_kwargs = {
                'folder_path' : folder_path,
                'dbs_path' : dbs_path
                },
    dag = dag
    )

# Функция для очистки папки проекта

clear_folders = PythonOperator(
    task_id='clear_folder', 
    python_callable=clear_folder, 
    op_kwargs={'folder': folder_path,
               'folder_not_delete' : 'xxx'}, 
    dag=dag
    )


procceess >> save_files_dbs >> clear_folders
    
