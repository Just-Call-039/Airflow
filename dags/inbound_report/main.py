import datetime
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from inbound_report import download_date_mysql, merge_df, save_result_to_click, download_date_clickhouse
from commons_liza.clear_folder import clear_folder, clear_unique_file
from commons_liza import dbs

default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='inbound_report',
    schedule_interval='20 4 * * *',
    start_date=pendulum.datetime(2023, 7, 25, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

numdays = 30

date_i = datetime.date.today() - datetime.timedelta(days=2)
year = date_i.year
month = date_i.month
day = date_i.day

date_before = date_i - datetime.timedelta(days=numdays)
year_date_before = date_before.year
month_date_before = date_before.month
day_date_before = date_before.day



cloud_truba = ['Kuzmenko', 'KZY26KpynLWQORNzkDlI5lu3ue7mtyKj', '192.168.1.40', 'asteriskcdrdb']
# cloud_call = ['base_dep_slave', 'IyHBh9mDBdpg','192.168.1.182', 'suitecrm']
# cloud_robot = ['base_dep_slave', 'IyHBh9mDBdpg','192.168.1.182', 'suitecrm_robot']
cloud_call = ['base_dep_slave', 'IyHBh9mDBdpg','192.168.1.183', 'suitecrm']
cloud_robot = ['base_dep_slave', 'IyHBh9mDBdpg','192.168.1.183', 'suitecrm_robot']

# Путь к файлам проекта

file_path = '/root/airflow/dags/inbound_report/Files/'

# Путь к шагам

step_path = '/root/airflow/dags/project_defenition/projects/steps/'

# Путь к inbound_call для архива

inbound_path = '/root/airflow/dags/inbound_report/Files/inbound_arhive/'

# Пути к sql запросам

truba_sql_path = '/root/airflow/dags/inbound_report/SQL/get_truba.sql'
robot_sql_path = '/root/airflow/dags/inbound_report/SQL/get_robotlog.sql'
astin_sql_path = '/root/airflow/dags/inbound_report/SQL/get_astin.sql'
inbound_sql_path = '/root/airflow/dags/inbound_report/SQL/get_inbound.sql'
call_sql_path = '/root/airflow/dags/inbound_report/SQL/get_call.sql'
request_sql_path = '/root/airflow/dags/inbound_report/SQL/get_request.sql'

# Путь к папке dbs

inbound_dbs_path = f'scripts fsp\Current Files\inbound_report\inbound_call\inbound_call_{year:02}_{month:02}_{day:02}.csv'
result_dbs_path = f'scripts fsp\Current Files\inbound_report\inbound_sheme\inbound_sheme.csv'

# Названия файлов

# truba_csv = f'truba_{year:02}_{month:02}_{day:02}.csv'
# astin_csv = f'astin_{year:02}_{month:02}_{day:02}.csv'
# inbound_csv = f'inbound_call_{year:02}_{month:02}_{day:02}.csv'

truba_csv = f'truba.csv'
astin_csv = f'astin.csv'
call_csv = f'call.csv'
robot_csv = f'robot.csv'
request_csv = 'request.csv'

inbound_csv = f'inbound_call_{year:02}_{month:02}_{day:02}.csv'

# Название файла inbound на удаление

inbound_csv_delete = f'inbound_call_{year_date_before:02}_{month_date_before:02}_{day_date_before:02}.csv'

# Названия рабочих файлов

inbound_truba_csv = f'inbound_truba.csv'
astin_truba_csv = f'astin_truba.csv'
robot_truba_csv = f'robot_truba.csv'
call_truba_csv = f'call_truba.csv'

# inbound_truba_csv = f'inbound_truba_{year:02}_{month:02}_{day:02}.csv'
# astin_truba_csv = f'astin_truba_{year:02}_{month:02}_{day:02}.csv'
# robot_truba_csv = f'robot_truba_{year:02}_{month:02}_{day:02}.csv'
# call_truba_csv = f'call_truba_{year:02}_{month:02}_{day:02}.csv'


type_dict = {'userfield' : 'str',
             'phone' : 'str',
             'uniqueid' : 'str',
             'billsec_t' : 'int64', 
             'billsec_a' : 'int64',
             'billsec_r' : 'int64',
             'exit_point' : 'str',
             'exit_name' : 'str',
             'queue_r' : 'str',
             'queue_c' : 'str',
             'queue_i' : 'str',
             'hour' : 'int64',
             'minute' : 'int64',
             'active_robot' : 'int64',
             'active_operator' : 'int64',
             'lastapp_Dial' : 'int64',
             'last_step' : 'str',
             'NN' : 'int64',
             
             'did' : 'str',
             'userid' : 'str'  
            }


get_truba = PythonOperator(
    task_id = 'get_truba',
    python_callable = download_date_mysql.get_data_permonth,
    op_kwargs = {'sql_download' : truba_sql_path,
                'cloud' : cloud_truba, 
                'date_before' : date_before,
                'date_i' :  str(date_i),
                'file_path' : f'{file_path}{truba_csv}'
                },
    dag = dag
    )     

get_robotlog = PythonOperator(
    task_id = 'get_robotlog',
    python_callable = download_date_mysql.get_robotlog,
    op_kwargs = {'sql_download' : robot_sql_path,
                'cloud' : cloud_robot, 
                'date_before' : date_before,
                'date_i' :  str(date_i),
                'file_path' : f'{file_path}{robot_csv}'
                },
    dag = dag
    )   

get_inbound = PythonOperator(
    task_id = 'get_inbound',
    python_callable = download_date_mysql.get_data,
    op_kwargs = {'sql_download' : inbound_sql_path,
                'cloud' : cloud_call,
                'date_i' : str(date_i),
                'file_path' : f'{inbound_path}{inbound_csv}'
                },
    dag = dag
    )      

get_astin = PythonOperator(
    task_id = 'get_astin',
    python_callable = download_date_clickhouse.get_data_ch,
    op_kwargs = {'sql_download' : astin_sql_path,
                 'date_before' : date_before,
                 'date_i' : date_i,
                 'file_path' : f'{file_path}{astin_csv}'
                },
    dag = dag
    )   

get_call = PythonOperator(
    task_id = 'get_call',
    python_callable = download_date_mysql.get_data_permonth,
    op_kwargs = {'sql_download' : call_sql_path,
                'cloud' : cloud_call, 
                'date_before' : date_before,
                'date_i' :  str(date_i),
                'file_path' : f'{file_path}{call_csv}'
                },
    dag = dag
    )  

get_request = PythonOperator(
    task_id = 'get_request',
    python_callable = download_date_mysql.get_data,
    op_kwargs = {'sql_download' : request_sql_path,
                'cloud' : cloud_call, 
                'date_i' :  str(date_i),
                'file_path' : f'{file_path}{request_csv}'
                },
    dag = dag
    ) 


union_with_inbound = PythonOperator(
    task_id = 'union_with_inbound',
    python_callable = merge_df.union_with_inbound,
    op_kwargs = {'truba_path' : f'{file_path}{truba_csv}',
                'inbound_path' : inbound_path, 
                'type_dict' : type_dict,
                'result_path' :f'{file_path}{inbound_truba_csv}'
                },
    dag = dag
    ) 

download_exit_dict = PythonOperator(
    task_id = 'download_exit_dict',
    python_callable = merge_df.union_exit_dict,
    op_kwargs = {
                'start_path' : f'{file_path}{inbound_truba_csv}',
                'result_path' : f'{file_path}{inbound_truba_csv}',
                'type_dict' : type_dict
                },
    dag = dag
    )  

union_astin_df = PythonOperator(
    task_id = 'union_astin_df',
    python_callable = merge_df.union_with_astin,
    op_kwargs = {'truba_path' : f'{file_path}{inbound_truba_csv}',
                'astin_path' : f'{file_path}{astin_csv}', 
                'type_dict' : type_dict,
                'result_path' : f'{file_path}{astin_truba_csv}'
                
                },
    dag = dag
    ) 


union_robot_df = PythonOperator(
    task_id = 'union_robot_df',
    python_callable = merge_df.union_robot_df,
    op_kwargs = {'start_path' : f'{file_path}{astin_truba_csv}',
                'robot_path' : f'{file_path}{robot_csv}', 
                'step_path' : step_path,
                'result_path' : f'{file_path}{robot_truba_csv}',
                'type_dict' : type_dict,
                'date_i' : date_i,
                'numdays' : numdays
                },
    dag = dag
    )        

       

union_call_df = PythonOperator(
    task_id = 'union_call_df',
    python_callable = merge_df.union_call_df,
    op_kwargs = {'start_path' : f'{file_path}{robot_truba_csv}',
                'call_path' : f'{file_path}{call_csv}', 
                'request_path' : f'{file_path}{request_csv}', 
                'result_path' : f'{file_path}{call_truba_csv}',
                'type_dict' : type_dict,
                'date_i' : date_i
                },
    dag = dag
    )   

  

save_result_click = PythonOperator(
    task_id = 'save_result_click',
    python_callable = save_result_to_click.save_data,
    op_kwargs = {
                'result_path' : f'{file_path}{call_truba_csv}',
                'type_dict' : type_dict,
                'date_before' : date_before
                },
    dag = dag
    )

save_request_click = PythonOperator(
    task_id = 'save_request_click',
    python_callable = save_result_to_click.save_request,
    op_kwargs = {
                'result_path' : f'{file_path}{request_csv}'
                },
    dag = dag
    )

save_inbound_dbs = PythonOperator(
    task_id = 'save_inbound_dbs',
    python_callable = dbs.save_file_to_dbs,
    op_kwargs = {
                'path_from' : f'{inbound_path}{inbound_csv}',
                'path_to' : inbound_dbs_path
                },
    dag = dag
    )

save_result_dbs = PythonOperator(
    task_id = 'save_result_dbs',
    python_callable = dbs.save_file_to_dbs,
    op_kwargs = {
                'path_from' : f'{file_path}{call_truba_csv}',
                'path_to' : result_dbs_path
                },
    dag = dag
    )



clear_folders = PythonOperator(
    task_id='clear_whole_folders', 
    python_callable=clear_folder, 
    op_kwargs={'folder': file_path,
               'folder_not_delete' : 'inbound_arhive'}, 
    dag=dag
    )

clear_inbound = PythonOperator(
    task_id='clear_inbound', 
    python_callable=clear_unique_file, 
    op_kwargs={'folder': inbound_path,
               'file_name' : inbound_csv_delete}, 
    dag=dag
    )

    
[get_truba, get_robotlog, get_inbound, get_call, get_request] >>\
      union_with_inbound >>  download_exit_dict >> union_astin_df >> union_robot_df >> union_call_df \
      >> save_result_click >> save_request_click >> [save_inbound_dbs, save_result_dbs] >> clear_folders >> clear_inbound