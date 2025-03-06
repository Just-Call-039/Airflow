import pendulum

from datetime import timedelta

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator
from base import proccess

default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='base',
    schedule_interval='0 3 * * 0',
    start_date=pendulum.datetime(2023, 4, 24, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud_name = 'cloud_128'
# cloud_name = 'cloud_183'

timeout = 120

# Путь к SQL запросу

path_sql_file = '/root/airflow/dags/base/SQL/base.sql'

# Путь к SQL запросу для извылечения данных по базе контаков
sql_clickload = '/root/airflow/dags/base/SQL/load_to_click.sql'

# Путь к датафрейму со значением городов
path_city = '/root/airflow/dags/base/Files/city.csv'

# Лимиты для выгрузки и загрузки данных, чтобы ничего не упало

list_limit = [40000000, 8000000, 160000000, 20000000, 240000000]

# Номер строки, с которой начнется выгрузка
start = 20000000

# Количество строк, которые будут выгружены
end = 10000000

# Словарь для определения приоритетов
dict_project = {'' : 'Пусто', 
                'ttk' : 'TTK',
                'bln' : 'BEELINE',
                'rtk' : 'RTK',
                'dom' : 'DOMRU',
                'mts' : 'MTS',
                'nbn' : 'NBN',
                'tat' : 'TAT',
                '2co': '2CO'}

# Список источников

source_list = ['^220^','^221^','^222^','^223^','^224^','^225^','^60^','^61^','^62^','^63^','^75^','^73^','^72^','^71^',
               '^70^','^69^','^68^','^32^','^204^','^308^','^203^','^207^','^211^','^208^','^30^','^31^','^33^','^217^',
               '^10^','^15^','^205^','^210^','^216^','^218^','^200^','^309^','^301^','^303^','^302^','^304^','^331^','^332^',
               '^201^','^202^','^206^','^209^','^305^','^306^','^307^','^300^','^310^','^311^','^312^','^313^','^314^','^315^',
               '^316^','^317^','^318^','^319^','^320^','^321^','^322^','^323^','^324^','^325^','^326^','^327^','^328^','^329^',
               '^330^','^400^','^401^','^402^','^403^','^404^','^405^','^333^','^334^','^336^','^337^','^338^','^339^','^340^',
               '^341^','^342^','^343^','^344^','^345^','^346^','^347^','^348^','^349^','^350^', 'Пусто']

# Список шагов

step_list = ['104', '105', '106', '107', '108', '103', '499', '903', '904', '918', '919', '920', '921', '922', '923', '111', '261',
             '262', '361', '362', '371', '372']

# Выгрузка, обработка и загрузка в CH

all_proccess = PythonOperator(
    task_id = 'all_proccess', 
    python_callable = proccess.data_load, 
    op_kwargs = {
                'start' : start,
                'end' : end,
                'cloud' : cloud_name, 
                'sql_file' : path_sql_file,
                'path_city' : path_city, 
                'dict_project' : dict_project, 
                'source_list' : source_list, 
                'step_list' : step_list, 
                'table_name' : 'contacts', 
                'table_name_ch' : 'contact_datebase', 
                'sql_download' : sql_clickload, 
                'timeout' : timeout}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
    task_id='send_telegram_message',
    telegram_conn_id='Telegram',
    chat_id='-1001412983860',
    text='Ошибка в модуле при выгрузке данных в базу контактов clickhouse',
    dag=dag,
    trigger_rule='one_failed'
)

all_proccess >> send_telegram_message
