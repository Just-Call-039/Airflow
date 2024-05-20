import datetime
import dateutil.relativedelta
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv
from commons_li.sql_query_semicolon_to_csv import sql_query_to_csv_sc
from commons_li.clear_folder import clear_folder
from previous_month.transfer_in_clickhouse import to_click


default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='previous_month',
    schedule_interval='20 5 1 * *',
    start_date=pendulum.datetime(2023, 9, 30, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

path_to_file_airflow = '/root/airflow/dags/previous_month/Files/'
path_airflow_10_otchet = f'{path_to_file_airflow}10_otchet/'
path_airflow_working = f'{path_to_file_airflow}working/'
path_airflow_calls = f'{path_to_file_airflow}calls/'
path_airflow_calls_with_request = f'{path_to_file_airflow}calls_with_request/'
path_airflow_transfer = '/root/airflow/dags/indicators_to_regions/Files/transfer/'
path_airflow_c = '/root/airflow/dags/indicators_to_regions/Files/sql_files/callls/'

path_dbs_calls_with_request = f'/4_report/new files/Звонки для заявок/'
path_dbs_calls = '/4_report/new files/calls/'
path_dbs_working = '/4_report/Files/working_time_folder/'
path_dbs_transfer = '/Отчеты BI/Показатели до регионов/TransferRobot/'
path_dbs_10_otchet = '/10_otchet_partners/Calls/'
path_dbs_c = '/Отчеты BI/Показатели до регионов/Для архива/'


sql_main_10_otchet = '/root/airflow/dags/previous_month/SQL/10_calls_previous_month.sql'
sql_main_transfer = '/root/airflow/dags/previous_month/SQL/transfer_robot_previous_month.sql'
sql_main_working = '/root/airflow/dags/previous_month/SQL/working_time_previous_month.sql'
sql_main_calls_with_request = '/root/airflow/dags/previous_month/SQL/calls_with_request_previous_month.sql'
sql_main_calls = '/root/airflow/dags/previous_month/SQL/calls_previous_month.sql'
sql_main_c = '/root/airflow/dags/previous_month/SQL/Call.sql'

today = datetime.date.today()
previous_date = today - dateutil.relativedelta.relativedelta(months=1)
year = previous_date.year
month = previous_date.month
file_calls = f'Звонки_{year}_{month}.csv'
file_otchet = f'{year}_{month}.csv'
file_calls_req = f'Звонки для заявок {month}{year}.csv'
file_work = f'{year}_{month}.csv'
file_transfer = f'Transfer {month}{year}.csv'
file_c = f'calls {month:02}_{year}.csv'



clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder, 
    op_kwargs={'folder': path_airflow_calls}, 
    dag=dag
    )
# Блок выполнения SQL запросов.
otchet_sql = PythonOperator(
    task_id='otchet_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main_10_otchet, 'path_csv_file': path_airflow_10_otchet, 'name_csv_file': file_otchet}, 
    dag=dag
    )
calls_sql = PythonOperator(
    task_id='calls_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main_calls, 'path_csv_file': path_airflow_calls, 'name_csv_file': file_calls}, 
    dag=dag
    )
calls_with_request_sql = PythonOperator(
    task_id='calls_with_request_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main_calls_with_request, 'path_csv_file': path_airflow_calls_with_request, 'name_csv_file': file_calls_req}, 
    dag=dag
    )
working_sql = PythonOperator(
    task_id='working_sql', 
    python_callable=sql_query_to_csv_sc, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main_working, 'path_csv_file': path_airflow_working, 'name_csv_file': file_work}, 
    dag=dag
    )
transfer_robot_sql = PythonOperator(
    task_id='transfer_robot_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main_transfer, 'path_csv_file': path_airflow_transfer, 'name_csv_file': file_transfer}, 
    dag=dag
    )
c_sql = PythonOperator(
    task_id='c_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main_c, 'path_csv_file': path_airflow_c, 'name_csv_file': file_c}, 
    dag=dag
    )


# Блок отправки всех файлов в папку DBS.
otchet_to_dbs = PythonOperator(
    task_id='otchet_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_10_otchet, 'to_path': path_dbs_10_otchet, 'file': file_otchet, 'db': 'DBS'}, 
    dag=dag
    )
calls_with_request_to_dbs = PythonOperator(
    task_id='calls_with_request_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_calls_with_request, 'to_path': path_dbs_calls_with_request, 'file': file_calls_req, 'db': 'DBS'}, 
    dag=dag
    )
calls_to_dbs = PythonOperator(
    task_id='calls_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_calls, 'to_path': path_dbs_calls, 'file': file_calls, 'db': 'DBS'}, 
    dag=dag
    )
working_to_dbs = PythonOperator(
    task_id='working_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_working, 'to_path': path_dbs_working, 'file': file_work, 'db': 'DBS'}, 
    dag=dag
    )
transfer_robot_to_dbs = PythonOperator(
    task_id='transfer_robot_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_transfer, 'to_path': path_dbs_transfer, 'file': file_transfer, 'db': 'DBS'}, 
    dag=dag
    )

c_to_dbs = PythonOperator(
    task_id='c_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_airflow_c, 'to_path': path_dbs_c, 'file': file_c, 'db': 'DBS'}, 
    dag=dag
    )
# Блок отправки  файлов в clickhouse.

transfer_to_click = PythonOperator(
    task_id='transfer_to_click', 
    python_callable=to_click, 
    op_kwargs={'path_file': path_airflow_calls, 'calls': file_calls}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Отчеты прошлого месяца выгружены',
        dag=dag,
        # on_failure_callback=True,
        # trigger_rule='all_success'
    )
send_telegram_message_fiasko = TelegramOperator(
        task_id='send_telegram_error',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Ошибки выгрузки отчетов прошлых месяцев',
        dag=dag,
        # on_failure_callback=True,
        trigger_rule='one_failed'
    )



# Блок очередности выполнения задач.
c_sql >> c_to_dbs >> [send_telegram_message, send_telegram_message_fiasko]
otchet_sql >> otchet_to_dbs >> [send_telegram_message, send_telegram_message_fiasko]
clear_folders >> calls_sql >> calls_to_dbs >> [send_telegram_message, send_telegram_message_fiasko]
transfer_robot_sql >> transfer_robot_to_dbs >> transfer_to_click >> [send_telegram_message, send_telegram_message_fiasko]
working_sql >> working_to_dbs >> [send_telegram_message, send_telegram_message_fiasko]
calls_with_request_sql >> calls_with_request_to_dbs >> [send_telegram_message, send_telegram_message_fiasko]

#[otchet_to_dbs, calls_to_dbs, transfer_robot_to_dbs, working_to_dbs, calls_with_request_to_dbs] >> send_telegram_message >> send_telegram_message_fiasko


