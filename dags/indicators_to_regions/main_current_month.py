from datetime import timedelta
from datetime import datetime
import pendulum
# import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator


from commons.transfer_file_to_dbs import transfer_file_to_dbs
from indicators_to_regions import regions_editer, to_clickhous, download_from_dbs, clear_folder, sql_query

default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='metrics_region_current',
    schedule_interval='30 5,7 2-31 * *',
    start_date=pendulum.datetime(2023, 8, 24, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud_name = 'cloud_128'
i_date = (datetime.now()).replace(day=1)

# Пути к запросам для выгрузки данных с базы

path_to_sql = '/root/airflow/dags/indicators_to_regions/SQL/'
sql_request = f'{path_to_sql}Request.sql'
sql_call = f'{path_to_sql}Call.sql'
sql_transfer = f'{path_to_sql}TransferRobot.sql'

# Пути к файлам проекта

path_to_file = '/root/airflow/dags/indicators_to_regions/Files/current_month/'
path_to_result = '/root/airflow/dags/indicators_to_regions/Files/current_month/result/'
path_to_request =  '/root/airflow/dags/indicators_to_regions/Files/'


# Пути к файлам для преобразования


path_to_call = f'{path_to_file}call/'
path_to_transfer = f'{path_to_file}transfer/'
path_to_user = '/root/airflow/dags/request_with_calls_today/Files/users.csv'
path_to_decoding = '/root/airflow/dags/current_month_yesterday/Files/decoding.xlsx'
path_to_workhour = '/root/airflow/dags/current_month_yesterday/Files/4/working_time_current_month.csv'


csv_call = 'call текущий.csv'
csv_transfer = 'transfer текущий.csv'
csv_request = 'request.csv'
csv_request_result = 'requests_total.csv'
csv_call_result = 'calltotal текущий.csv'
csv_call_ch = 'calltotal_current.csv'
csv_city = 'Город.csv'
csv_request_prev = 'requests_prev.csv'



# Пути к dbs

dbs_request = '/Отчеты BI/Показатели до регионов/Заявки/test/'
dbs_call = '/Отчеты BI/Показатели до регионов/CallsTotal/test/'
dbs_transfer = '/Отчеты BI/Показатели до регионов/TransferRobot/test/'
path_to_dbs = '/Отчеты BI/Стандартные справочники/'


# Блок выполнения SQL запросов.
request_sql = PythonOperator(
    task_id='request_sql', 
    python_callable=sql_query.sql_query_current, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_request, 'path_csv_file': path_to_result, 'name_csv_file': csv_request}, 
    dag=dag
    )

calls_sql = PythonOperator(
    task_id='calls_sql', 
    python_callable=sql_query.sql_query_current, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_call, 'path_csv_file': path_to_call, 'name_csv_file': csv_call}, 
    dag=dag
    )

transfer_sql = PythonOperator(
    task_id='transfer_sql', 
    python_callable=sql_query.sql_query_current, 
    op_kwargs={'cloud': cloud_name, 'path_sql_file': sql_transfer, 'path_csv_file': path_to_transfer, 'name_csv_file': csv_transfer}, 
    dag=dag
    )

download_city = PythonOperator(
    task_id = 'download_city',
    python_callable=download_from_dbs.transfer_file_from_dbs,
    op_kwargs={'file_path_on_share' : path_to_dbs,
                'local_file_path' : path_to_file,
                'file_name_list' : [csv_city]},
    dag=dag
    )

# download_request_prev = PythonOperator(
#     task_id = 'download_request_prev',
#     python_callable=download_from_dbs.transfer_file_from_dbs,
#     op_kwargs={'file_path_on_share' : path_to_dbs_1,
#                 'local_file_path' : path_to_file,
#                 'file_name_list' : [csv_request_prev]},
#     dag=dag
#     )

# Преобразование файлов после sql для dbs
region_editing = PythonOperator(
    task_id='region_editing', 
    python_callable=regions_editer.region_editer_per_month, 
    op_kwargs={'path_to_file': path_to_file,
               'file_request': csv_request,
               'path_to_request' : path_to_request,
               'file_request_prev' : csv_request_prev,
               'path_to_file_sql' : path_to_call, 
               'file_call': csv_call,
               'path_to_sql_transfer' : path_to_transfer,
               'csv_transfer' : csv_transfer,
               'path_result': path_to_result,
               'file_request_result': csv_request_result,
               'file_call_result': csv_call_result,
               'i_date' : i_date.strftime('%Y-%m-%d')
               }, 
    dag=dag
    )



# Преобразование файлов после sql для clickhous
call_edit_ch = PythonOperator(
    task_id='call_edit_ch', 
    python_callable=to_clickhous.call_union, 
    op_kwargs={'path_to_file': path_to_file,
               'csv_city' : csv_city,
               'path_to_request' : f'{path_to_result}{csv_request}',               
               'path_to_call' : f'{path_to_call}{csv_call}', 
               'path_to_transfer' : f'{path_to_transfer}{csv_transfer}', 
               'path_to_user' : path_to_user, 
               'path_to_decoding' : path_to_decoding, 
               'path_to_workhour' : path_to_workhour,
               'path_to_result'  : path_to_result,
               'file_name_result': csv_call_ch
               }, 
    dag=dag
    )

# Отправка в кликхаус
call_to_click = PythonOperator(
    task_id='to_click', 
    python_callable=to_clickhous.to_click_current, 
    op_kwargs={'path_to_result' : path_to_result, 
               'file_name' : csv_call_ch
            }, 
    dag=dag
    )

# call_to_click_test = PythonOperator(
#     task_id='to_click_test', 
#     python_callable=to_clickhous.test_to_click, 
#     op_kwargs={'path_to_result' : path_to_result, 
#                'file_name' : csv_call_ch
#             }, 
#     dag=dag
#     )


calls_to_dbs = PythonOperator(
    task_id='calls_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_result, 'to_path': dbs_call, 
               'file': csv_call_result, 'db': 'DBS'}, 
    dag=dag
    )
request_to_dbs = PythonOperator(
    task_id='request_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_request, 'to_path': dbs_request, 
               'file': csv_request_result, 'db': 'DBS'}, 
    dag=dag
    )
transfer_to_dbs = PythonOperator(
    task_id='transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_transfer, 'to_path': dbs_transfer, 'file': csv_transfer, 'db': 'DBS'}, 
    dag=dag
    )

clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder.clear_folder, 
    op_kwargs={'folder_list': [path_to_transfer, path_to_call, path_to_result, path_to_file]}, 
    dag=dag
    )



[request_sql, calls_sql, transfer_sql, download_city] \
    >> region_editing >> [calls_to_dbs, request_to_dbs, transfer_to_dbs]

[request_sql, calls_sql, transfer_sql, download_city] >> call_edit_ch >> [call_to_click]

[calls_to_dbs, request_to_dbs, transfer_to_dbs, call_to_click] >> clear_folders