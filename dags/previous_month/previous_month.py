import datetime
import dateutil.relativedelta
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv

default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='previous_month',
    schedule_interval='50 4 1 * *',
    start_date=pendulum.datetime(2023, 8, 1, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

path_to_file_airflow = '/root/airflow/dags/previous_month/Files/'
path_airflow_10_otchet = f'{path_to_file_airflow}10_otchet/'
path_airflow_transfer = f'{path_to_file_airflow}transfer_robot/'
path_airflow_working = f'{path_to_file_airflow}working/'
path_airflow_calls = f'{path_to_file_airflow}calls/'
path_airflow_calls_with_request = f'{path_to_file_airflow}calls_with_request/'


path_dbs_calls_with_request = f'/4_report/new files/Звонки для заявок/'
path_dbs_calls = '/4_report/new files/calls/'
path_dbs_working = '/4_report/Files/working_time_folder/'
path_dbs_transfer = '/Отчеты BI/Показатели до регионов/TransferRobot/'
path_dbs_10_otchet = '/10_otchet_partners/Calls/'

sql_main_10_otchet = '/root/airflow/dags/previous_month/SQL/10_calls_previous_month.sql'
sql_main_transfer = '/root/airflow/dags/previous_month/SQL/transfer_robot_previous_month.sql'
sql_main_working = '/root/airflow/dags/previous_month/SQL/working_time_previous_month.sql'
sql_main_calls_with_request = '/root/airflow/dags/previous_month/SQL/calls_with_request_previous_month.sql'
sql_main_calls = '/root/airflow/dags/previous_month/SQL/calls_previous_month.sql'

today = datetime.date.today()
previous_date = today - dateutil.relativedelta.relativedelta(months=1)
year = previous_date.year
month = previous_date.month
file_name = f'{month}_{year}.csv'

# Блок выполнения SQL запросов.
otchet_sql = PythonOperator(
    task_id='otchet_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main_10_otchet, 'path_csv_file': path_airflow_10_otchet, 'name_csv_file': file_name}, 
    dag=dag
    )
calls_sql = PythonOperator(
    task_id='calls_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main_calls, 'path_csv_file': path_airflow_calls, 'name_csv_file': file_name}, 
    dag=dag
    )
calls_with_request_sql = PythonOperator(
    task_id='calls_with_request_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main_calls_with_request, 'path_csv_file': path_airflow_calls_with_request, 'name_csv_file': file_name}, 
    dag=dag
    )
working_sql = PythonOperator(
    task_id='working_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main_working, 'path_csv_file': path_airflow_working, 'name_csv_file': file_name}, 
    dag=dag
    )
transfer_robot_sql = PythonOperator(
    task_id='transfer_robot_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main_transfer, 'path_csv_file': path_airflow_transfer, 'name_csv_file': file_name}, 
    dag=dag
    )


# Блок отправки всех файлов в папку DBS.
otchet_to_dbs = PythonOperator(
    task_id='otchet_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_10_otchet, 'to_path': path_dbs_10_otchet, 'file': file_name, 'db': 'DBS'}, 
    dag=dag
    )
calls_with_request_to_dbs = PythonOperator(
    task_id='calls_with_request_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_calls_with_request, 'to_path': path_dbs_calls_with_request, 'file': file_name, 'db': 'DBS'}, 
    dag=dag
    )
calls_to_dbs = PythonOperator(
    task_id='calls_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_calls, 'to_path': path_dbs_calls, 'file': file_name, 'db': 'DBS'}, 
    dag=dag
    )
working_to_dbs = PythonOperator(
    task_id='working_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_working, 'to_path': path_dbs_working, 'file': file_name, 'db': 'DBS'}, 
    dag=dag
    )
transfer_robot_to_dbs = PythonOperator(
    task_id='transfer_robot_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_transfer, 'to_path': path_dbs_transfer, 'file': file_name, 'db': 'DBS'}, 
    dag=dag
    )


# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Все отчеты за прошлый месяц сформированы!',
        dag=dag,
        # on_failure_callback=True,
        # trigger_rule='all_success'
        trigger_rule='one_failed'
    )


# Блок очередности выполнения задач.
otchet_sql >> otchet_to_dbs 
calls_sql >> calls_to_dbs 
transfer_robot_sql >> transfer_robot_to_dbs 
working_sql >> working_to_dbs 
calls_with_request_sql >> calls_with_request_to_dbs 

[otchet_to_dbs, calls_to_dbs, transfer_robot_to_dbs, working_to_dbs, calls_with_request_to_dbs] >> send_telegram_message


