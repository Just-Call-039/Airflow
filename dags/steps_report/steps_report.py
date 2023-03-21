import pendulum
import dateutil.relativedelta
from datetime import timedelta, date

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from commons.sql_query_to_csv import sql_query_to_csv
from steps_report.main_transformation import main_transformation
from steps_report.create_medium_step_file import create_medium_step_file


default_args = {
    'owner': 'Alexander Brezhnev',
    'email': 'brezhnev.aleksandr@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='steps_report',
    schedule_interval='30 6,12 * * *',
    start_date=pendulum.datetime(2023, 3, 13, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

today = date.today()
previous_date = today - dateutil.relativedelta.relativedelta(months=1)

year_for_previous = previous_date.year
month_for_previous = previous_date.month
file_name_previous = f'{year_for_previous}_{month_for_previous}.csv'

year_for_current = today.year
month_for_current = today.month
file_name_current = f'{year_for_current}_{month_for_current}.csv'

previous_day = today - dateutil.relativedelta.relativedelta(days=1)
year_main = previous_day.year
month_main = previous_day.month
day_main = previous_day.day
file_name_main = f'{year_main}_{month_main}_{day_main}.csv'

path_to_file_airflow = '/root/airflow/dags/steps_report/files/'
path_to_files_from_sql = f'{path_to_file_airflow}files_from_sql/'
path_to_main_folder = f'{path_to_file_airflow}main_folder/'
path_to_requests_folder = f'{path_to_file_airflow}requests_folder/'
path_to_uniqueid_medium_folder = f'{path_to_file_airflow}uniqueid_medium_folder/'

path_to_sql_airflow = '/root/airflow/dags/steps_report/SQL/'
sql_main = f'{path_to_sql_airflow}main.sql'
sql_requests_previous_month = f'{path_to_sql_airflow}requests_previous_month.sql'
sql_requests_current_month = f'{path_to_sql_airflow}requests_current_month.sql'

path_to_file_dbs = '/steps_report/files/'
dbs_from_sql = f'{path_to_file_dbs}files_from_sql/'
dbs_main_folder = f'{path_to_file_dbs}main_folder/'
dbs_requests_folder = f'{path_to_file_dbs}requests_folder/'
dbs_uniqueid_medium_folder = f'{path_to_file_dbs}uniqueid_medium_folder/'

# Блок выполнения SQL запросов.
requests_previous_month_sql = PythonOperator(
    task_id='requests_previous_month_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_requests_previous_month, 'path_csv_file': path_to_requests_folder, 'name_csv_file': file_name_previous}, 
    dag=dag
    )
requests_current_month_sql = PythonOperator(
    task_id='requests_current_month_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_requests_current_month, 'path_csv_file': path_to_requests_folder, 'name_csv_file': file_name_current}, 
    dag=dag
    )
main_folder_sql = PythonOperator(
    task_id='main_folder_sql', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_main, 'path_csv_file': path_to_files_from_sql, 'name_csv_file': file_name_main}, 
    dag=dag
    )

# Преобразование файлов из запроса Main.
main_transformation_operator = PythonOperator(
    task_id='main_transformation', 
    python_callable=main_transformation, 
    op_kwargs={'name': file_name_main, 'files_from_sql': path_to_files_from_sql, 'main_folder': path_to_main_folder}, 
    dag=dag
    )
create_medium_step_file_operator = PythonOperator(
    task_id='create_medium_step_file', 
    python_callable=create_medium_step_file, 
    op_kwargs={'name': file_name_main, 'files_from_sql': path_to_files_from_sql, 'uniqueid_medium_folder': path_to_uniqueid_medium_folder}, 
    dag=dag
    )

# Блок отправки всех файлов в папку DBS.
requests_previous_month_to_dbs = PythonOperator(
    task_id='requests_previous_month_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_requests_folder, 'to_path': dbs_requests_folder, 'file': file_name_previous, 'db': 'DBS'}, 
    dag=dag
    )
requests_current_month_to_dbs = PythonOperator(
    task_id='requests_current_month_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_requests_folder, 'to_path': dbs_requests_folder, 'file': file_name_current, 'db': 'DBS'}, 
    dag=dag
    )
main_folder_to_dbs = PythonOperator(
    task_id='main_folder_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_files_from_sql, 'to_path': dbs_from_sql, 'file': file_name_main, 'db': 'DBS'}, 
    dag=dag
    )
main_folder_true_to_dbs = PythonOperator(
    task_id='main_folder_true_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_main_folder, 'to_path': dbs_main_folder, 'file': file_name_main, 'db': 'DBS'}, 
    dag=dag
    )
medium_step_file_to_dbs = PythonOperator(
    task_id='medium_step_file_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_uniqueid_medium_folder, 'to_path': dbs_uniqueid_medium_folder, 'file': file_name_main, 'db': 'DBS'}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Произошла ошибка работы отчета по шагам.',
        dag=dag,
        # on_failure_callback=True,
        # trigger_rule='all_success'
        trigger_rule='one_failed'
    )

# Блок очередности выполнения задач.
requests_previous_month_sql >> requests_previous_month_to_dbs
requests_current_month_sql >> requests_current_month_to_dbs
main_folder_sql >> [main_transformation_operator, create_medium_step_file_operator, main_folder_to_dbs]
main_transformation_operator >> main_folder_true_to_dbs
create_medium_step_file_operator >> medium_step_file_to_dbs
[requests_current_month_to_dbs, requests_previous_month_to_dbs, main_folder_true_to_dbs, medium_step_file_to_dbs] >> send_telegram_message
