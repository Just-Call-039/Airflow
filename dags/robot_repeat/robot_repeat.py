import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from commons import transfer_file_to_dbs

from robot_repeat import processing
from route_robotlogs import clear_folder
from commons import transfer_file_to_dbs

default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='robot_repeat',
    schedule_interval='0 3 * * 0',
    start_date=pendulum.datetime(2023, 7, 25, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )

cloud_name = 'cloud_128'

hosts = {
    58 : '192.168.1.116',
    52: '192.168.1.109',
    27 : '192.168.1.86'
}


# Пути к файлам этого проекта на сервере airflow

path_project_folder = '/root/airflow/dags/robot_repeat/Files/'

calls = 'robot_repeat.csv'
name_df_words = 'words_group.csv'

# Пути к файлам на dbs

path_project_dbs = 'scripts fsp/Current Files/robot_repeat/'

words_dict = {

    'не понятно' : ['непонял', 'непонятно', 'что говорите', 'непонимаю'],
    'плохо слышно' :['неслышно', 'плохо слышно', 'плохо слышу', 'алло девушка', 'неслышу'],
    'область/город/район': ['область', 'район', 'город', 'башкортостан'],
    'негород' : ['негород', 'несело', 'недеревня', 'яневгороде', 'нев городе'],
    'называет оператора' : ['ростелеком', 'теле два', 'мтс', 'билайн', 'мегафон'],
    'приветствие' : ['здравс', 'привет', 'ну говорите',  'да говорите', 'слушаю'],
    'согласие' : [' да ', 'угу', 'понятно', 'ясно '],
    'нет возможности подключения' : ['уже смотрели', 'наш дом '],
    'уже подключен': ['уже подклю', 'я и так '],
    'автоответчик' : ['здравствуйте с вами говорит', 'первый сезон нашего разговора', 'готов говорить с вами',\
                      'здравствуйте вы позвонили', 'невполне понял', 'окей записал еще что то передать',\
                      'незнаю нужно ли мне что то такое мне и так']}

# Выполнение заданий

# Загрузка датасетов

download_calls = PythonOperator(
    task_id='download_calls',
    python_callable=processing.download_from_db,
    op_kwargs={'host_dict' : hosts, 'path_to_folder': path_project_folder, 'file_name' : calls},
    dag=dag
    )

create_words_df = PythonOperator(
    task_id='create_words_df',
    python_callable=processing.create_words_df,
    op_kwargs={'path_to_folder' : path_project_folder, 'file_name' : name_df_words},
    dag=dag
    )

add_col_word = PythonOperator(
    task_id='add_columns_word',
    python_callable=processing.add_col_words,
    op_kwargs={'path_to_folder' : path_project_folder, 'file_name' : calls},
    dag=dag
    )


# Блок отправки данных в папку DBS

transfer_calls = PythonOperator(
    task_id='transfer_calls', 
    python_callable=transfer_file_to_dbs.transfer_file_to_dbs, 
    op_kwargs={'from_path': path_project_folder, 'to_path': path_project_dbs, 'file': calls, 'db': 'DBS'}, 
    dag=dag
    )

transfer_df_words = PythonOperator(
    task_id='transfer_df_words', 
    python_callable=transfer_file_to_dbs.transfer_file_to_dbs, 
    op_kwargs={'from_path': path_project_folder, 'to_path': path_project_dbs, 'file': name_df_words, 'db': 'DBS'}, 
    dag=dag
    )

# Очистка папки

clear_folders = PythonOperator(
    task_id='clear_folders', 
    python_callable=clear_folder.clear_folder, 
    op_kwargs={'folder': path_project_folder}, 
    dag=dag
    )


download_calls >> add_col_word >> transfer_calls
create_words_df >> transfer_df_words
[transfer_calls, transfer_df_words] >> clear_folders



