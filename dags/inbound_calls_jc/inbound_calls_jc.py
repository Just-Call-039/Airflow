import pendulum
from datetime import timedelta, date

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from fsp.repeat_download import repeat_download
from fsp.repeat_download import sql_query_to_csv

# from airflow.utils.trigger_rule import TriggerRule
# from airflow.models import Variable



default_args = {
    'owner': 'Aleksandra Amelina',
    'email': 'sawakuzmenko18@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }


dag = DAG(
    dag_id='inbound_calls_jc',
    schedule_interval='00 08 * * *',
    start_date=pendulum.datetime(2023, 4, 24, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud_name = 'cloud_128'
cloud_name2 = 'Truby'
cloud_name3 = 'Click2'

n = 1
days = 1
source_truba = [1]

# Пути к sql запросам на сервере airflow
path_to_sql_airflow = '/root/airflow/dags/inbound_calls_jc/SQL/'
sql_truba = f'{path_to_sql_airflow}truba.sql'
sql_astin = f'{path_to_sql_airflow}astin.sql'
sql_inbound_scheme = f'{path_to_sql_airflow}inbound_scheme.sql'
sql_robotlog = f'{path_to_sql_airflow}robotlog.sql'
sql_operator = f'{path_to_sql_airflow}operator.sql'



# Наименование файлов
file_name_truba = 'users.csv'
file_name_astin = 'astin.csv'
file_name_inbound_scheme = 'inbound_scheme.csv'
file_name_robotlog = 'robotlog.csv'
file_name_operator = 'operator.csv'


# Пути к файлам на сервере airflow
# Сразу после sql
path_to_file_airflow = '/root/airflow/dags/inbound_calls_jc/Files/'




# Выполнение SQL запросов
sql_truba = PythonOperator(
    task_id='sql_truba',
    python_callable=repeat_download,
    op_kwargs={'n': n, 'days': days, 'source': source_truba, 'cloud': cloud_name2, 'path_sql_file': sql_truba, 'path_csv_file': path_to_file_airflow, 'name_csv_file': file_name_truba},
    dag=dag
    )

sql_astin = PythonOperator(
    task_id='sql_astin',
    python_callable=repeat_download,
    op_kwargs={'n': n, 'days': days, 'source': '', 'cloud': cloud_name3, 'path_sql_file': sql_astin, 'path_csv_file': path_to_file_airflow, 'name_csv_file': file_name_astin},
    dag=dag
    )

sql_inbound_scheme = PythonOperator(
    task_id='sql_inbound_scheme',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_inbound_scheme, 'path_csv_file': path_to_file_airflow, 'name_csv_file': file_name_inbound_scheme},
    dag=dag
    )

sql_robotlog = PythonOperator(
    task_id='sql_robotlog',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_robotlog, 'path_csv_file': path_to_file_airflow, 'name_csv_file': file_name_robotlog},
    dag=dag
    )

sql_operator = PythonOperator(
    task_id='sql_operator',
    python_callable=sql_query_to_csv,
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_operator, 'path_csv_file': path_to_file_airflow, 'name_csv_file': file_name_operator},
    dag=dag
    )




# # Преобразование файлов после sql.
# transformation_operational = PythonOperator(
#     task_id='operational_transformation', 
#     python_callable = operational_transformation, 
#     op_kwargs={'path_to_users': path_to_file_users, 'name_users': file_name_users, 'path_to_folder': path_to_sql_operational_folder,
#                 'name_calls': file_name_operational, 'path_to_final_folder': path_to_sql_operational_operativ}, 
#     dag=dag
#     )

# transformation_operational_calls = PythonOperator(
#     task_id='operational_calls_transformation', 
#     python_callable = operational_calls_transformation, 
#     op_kwargs={'path_to_folder': path_to_sql_operational_folder, 'name_calls': file_name_operational_calls, 'path_to_final_folder': path_to_operational_folder}, 
#     dag=dag
#     )


# # Перенос всех файлов в папку DBS.
# # transfer_operational = PythonOperator(
# #     task_id='operational_transfer', 
# #     python_callable=transfer_files_to_dbs, 
# #     op_kwargs={'from_path': path_to_operational_folder, 'to_path': dbs_operational, 'db': 'DBS'}, 
# #     dag=dag
# #     )

# transfer_operational2 = PythonOperator(
#     task_id='operational_transfer2', 
#     python_callable=transfer_files_to_dbs, 
#     op_kwargs={'from_path': path_to_operational_folder, 'to_path': dbs_operational2, 'db': 'DBS'}, 
#     dag=dag
#     )

# transfer_operational_main = PythonOperator(
#     task_id='operational_transfer_main', 
#     python_callable=transfer_files_to_dbs, 
#     op_kwargs={'from_path': path_to_sql_operational_operativ, 'to_path': dbs_operational3, 'db': 'DBS'}, 
#     dag=dag
#     )

# # Отправка уведомления об ошибке в Telegram.
# send_telegram_message = TelegramOperator(
#     task_id='send_telegram_message',
#     telegram_conn_id='Telegram',
#     chat_id='-1001412983860',
#     text='Ошибка в модуле при выгрузке данных для оперативных отчетов',
#     dag=dag,
#     trigger_rule='one_failed'
# )

# Очередности выполнения задач.



[sql_truba, sql_astin, sql_inbound_scheme, sql_robotlog, sql_operator]










