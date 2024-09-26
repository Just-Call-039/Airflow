from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator

from commons.transfer_file_to_dbs import transfer_file_to_dbs
from fsp.repeat_download import sql_query_to_csv
from commons_li.sql_query_semicolon_to_csv import sql_query_to_csv_sc

from current_month_yesterday.transfer_in_clickhouse import to_click
from current_month_yesterday import transfer_in_clickhouse_v2



default_args = {
    'owner': 'Lidiya Butenko',
    'email': 'lidiyaa.butenkoo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='yesterday_day_file',
    schedule_interval='30 6 * * *',
    start_date=pendulum.datetime(2023, 7, 13, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud_name = 'cloud_128'

# Наименование файлов.
csv_calls = 'Звонки_вчера.csv' 
csv_call_wait = 'CallWaitUser.csv' 
csv_login_user = 'LoginUsers.csv'
csv_user = 'users.csv'
csv_user_total = 'Users_total.csv' 
csv_working = 'working_time_current_month.csv' 

# Пути к sql запросам на сервере airflow.
path_to_sql_airflow = '/root/airflow/dags/current_month_yesterday/SQL/'
sql_calls = f'{path_to_sql_airflow}calls_yesterday.sql'
sql_calls_4 = f'{path_to_sql_airflow}Звонки вчера.sql'
sql_users_total = f'{path_to_sql_airflow}Users_total.sql'
sql_call_wait = f'{path_to_sql_airflow}CallWaitUser.sql'
sql_login_users = f'{path_to_sql_airflow}LoginUsers.sql'
sql_working = f'{path_to_sql_airflow}working_time_month_any.sql'



# Пути к файлам на сервере airflow.
path_to_file_airflow = '/root/airflow/dags/current_month_yesterday/Files/'
path_to_file_sql = f'{path_to_file_airflow}4/'
path_to_file_user = '/root/airflow/dags/request_with_calls_today/Files/'

# Пути к файлам на сервере dbs.
path_to_file_dbs = '/10_otchet_partners/Calls/'
path_to_file_dbs_4 = '/4_report/new files/calls/'
path_to_user_dop_dbs = '/4_report/'
path_to_work_dbs = '/4_report/Files/working_time_folder/'



# Блок выполнения SQL запросов.

calls_yesterday = PythonOperator(
    task_id='calls_yesterday', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_calls, 'path_csv_file': path_to_file_airflow, 'name_csv_file': csv_calls}, 
    dag=dag
    )

calls_yesterday_4 = PythonOperator(
    task_id='calls_yesterday_4', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_calls_4, 'path_csv_file': path_to_file_sql, 'name_csv_file': csv_calls}, 
    dag=dag
    )
login_user = PythonOperator(
    task_id='login_user', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_login_users, 'path_csv_file': path_to_file_sql, 'name_csv_file': csv_login_user}, 
    dag=dag
    )
users_total = PythonOperator(
    task_id='users_total', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_users_total, 'path_csv_file': path_to_file_sql, 'name_csv_file': csv_user_total}, 
    dag=dag
    )
call_wait = PythonOperator(
    task_id='call_wait', 
    python_callable=sql_query_to_csv, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_call_wait, 'path_csv_file': path_to_file_sql, 'name_csv_file': csv_call_wait}, 
    dag=dag
    )
working_time = PythonOperator(
    task_id='working_time', 
    python_callable=sql_query_to_csv_sc, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_working, 'path_csv_file': path_to_file_sql, 'name_csv_file': csv_working}, 
    dag=dag
    )



# Блок отправки всех файлов в папку DBS.

calls_yesterday_to_dbs = PythonOperator(
    task_id='calls_yesterday_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': csv_calls, 'db': 'DBS'}, 
    dag=dag
    )
calls_yesterday_4_to_dbs = PythonOperator(
    task_id='calls_yesterday_4_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_sql, 'to_path': path_to_file_dbs_4, 'file': csv_calls, 'db': 'DBS'}, 
    dag=dag
    )
user_total_to_dbs = PythonOperator(
    task_id='user_total_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_sql, 'to_path': path_to_user_dop_dbs, 'file': csv_user_total, 'db': 'DBS'}, 
    dag=dag
    )
login_user_to_dbs = PythonOperator(
    task_id='login_user_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_sql, 'to_path': path_to_user_dop_dbs, 'file': csv_login_user, 'db': 'DBS'}, 
    dag=dag
    )
call_wait_to_dbs = PythonOperator(
    task_id='call_wait_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_sql, 'to_path': path_to_user_dop_dbs, 'file': csv_call_wait, 'db': 'DBS'}, 
    dag=dag
    )
work_time_to_dbs = PythonOperator(
    task_id='work_time_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_sql, 'to_path': path_to_work_dbs, 'file': csv_working, 'db': 'DBS'}, 
    dag=dag
    )

# Блок отправки  файлов в clickhouse.

transfer_to_click = PythonOperator(
    task_id='transfer_to_click', 
    python_callable=to_click, 
    op_kwargs={'path_file': path_to_file_sql, 'calls': csv_calls}, 
    dag=dag
    )

transfer_call_to_click = PythonOperator(
    task_id='call_to_click', 
    python_callable=transfer_in_clickhouse_v2.call_to_click, 
    op_kwargs={'path_file': path_to_file_sql, 'call' : csv_calls}, 
    dag=dag
    )

transfer_worktime_to_click = PythonOperator(
    task_id='worktime_to_click', 
    python_callable=transfer_in_clickhouse_v2.work_to_click, 
    op_kwargs={'path_file': path_to_file_sql, 'work_hour' : csv_working}, 
    dag=dag
    )

transfer_usertotal_to_click = PythonOperator(
    task_id='usertotal_to_click', 
    python_callable=transfer_in_clickhouse_v2.usertotal_to_click, 
    op_kwargs={'path_file': path_to_file_sql, 'usertotal' : csv_user_total}, 
    dag=dag
    )

transfer_userlogin_to_click = PythonOperator(
    task_id='userlogin_to_click', 
    python_callable=transfer_in_clickhouse_v2.userlogin_to_click, 
    op_kwargs={'path_file': path_to_file_sql, 'userlogin' : csv_login_user}, 
    dag=dag
    )

transfer_user_to_clickhous = PythonOperator(
    task_id='user_to_clickhous', 
    python_callable=transfer_in_clickhouse_v2.user_to_click, 
    op_kwargs={'path_file': path_to_file_user, 'user' : csv_user}, 
    dag=dag
    )

transfer_call10_to_clickhous = PythonOperator(
    task_id='call_10_to_click', 
    python_callable=transfer_in_clickhouse_v2.call_10_to_click, 
    op_kwargs={'path_file': path_to_file_airflow, 'call_10' : csv_calls}, 
    dag=dag
    )

transfer_callwait_to_clickhous = PythonOperator(
    task_id='callwait_to_click', 
    python_callable=transfer_in_clickhouse_v2.callwait_to_click, 
    op_kwargs={'path_file': path_to_file_sql, 'callwait' : csv_call_wait}, 
    dag=dag
    )

calls_yesterday >> [calls_yesterday_to_dbs, transfer_call10_to_clickhous]
calls_yesterday_4 >> calls_yesterday_4_to_dbs >> [transfer_to_click, transfer_call_to_click]
login_user >> [login_user_to_dbs, transfer_userlogin_to_click]
users_total >> [user_total_to_dbs, transfer_usertotal_to_click]
call_wait >> [call_wait_to_dbs, transfer_callwait_to_clickhous]
working_time >> [work_time_to_dbs, transfer_worktime_to_click]
transfer_user_to_clickhous



























