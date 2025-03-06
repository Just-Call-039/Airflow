import datetime
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator
from robot_debug import proccess

from route_robotlogs.clear_folder import clear_folder



default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='robot_debug',
    schedule_interval='0 4 * * *',
    start_date=pendulum.datetime(2023, 7, 25, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud = 'cloud_128'
# cloud = 'cloud_183'

# Путь к данным для подключения к Clickhouse
cloud_ch = '/root/airflow/dags/not_share/ClickHouse2.csv'

# Путь к запросу к базе Mysql

path_sql_file = '/root/airflow/dags/robot_debug/SQL/get_request.sql'

# Наименование путей к папкам проекта
path_to_folder = '/root/airflow/dags/robot_debug/Files/'
path_to_file = f'{path_to_folder}debug_parse.csv'

# Название таблицы на сlickhouse

table_name = 'robot_debug_parser'

# Загружаем датасет за предыдущий день 

get_df = PythonOperator(
    task_id = 'get_df',
    python_callable = proccess.download_data_request,
    op_kwargs = {'cloud' : cloud,
                'path_sql_request' : path_sql_file,
                'path_to_file' : path_to_file 
                },
    dag = dag
    )
# Отправляем данные в сlickhouse

to_click = PythonOperator(
    task_id = 'to_click',
    python_callable = proccess.save_table,
    op_kwargs = {'cloud_ch' : cloud_ch,
                'table_name' : table_name,
                'path_to_file' : path_to_file 
                },
    dag = dag
    )

# # Очищаем папку

clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder, 
    op_kwargs={'folder': path_to_folder}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram

send_telegram_message = TelegramOperator(
    task_id='send_telegram_message',
    telegram_conn_id='Telegram',
    chat_id='-1001412983860',
    text='Ошибка в модуле при выгрузке данных debug_parse',
    dag=dag,
    trigger_rule='one_failed'
)

get_df >> to_click >> clear_folders >> send_telegram_message
