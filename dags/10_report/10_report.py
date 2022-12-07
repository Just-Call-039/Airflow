from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import PythonOperator

from commons.clear_file import clear_file
from commons.transfer_file import transfer_file
from commons.transfer_file_to_dbs import transfer_file_to_dbs
from commons.del_file import del_file


default_args = {
    'owner': 'Alexander Brezhnev',
    'email': 'brezhnev.aleksandr@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'mysql_conn_id': 'cloud_my_sql_117',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id='10_report',
    schedule_interval='50 5-13/4 * * *',
    start_date=pendulum.datetime(2022, 6, 16, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


path_to_file_airflow = '/root/airflow/dags/10_report/Files/'
path_to_file_mysql = '/home/glotov/192.168.1.117/10_report/'
path_to_file_dbs = '/10_report/Files/'
path_to_file_dbs_4_rep = '/4_report/Files/'

# Блок предварительного удаления файлов с сервера.
all_users_del = PythonOperator(
    task_id='all_users_del', 
    python_callable=del_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'file': 'All_users.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
super_del = PythonOperator(
    task_id='super_del', 
    python_callable=del_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'file': 'Super.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
total_calls_del = PythonOperator(
    task_id='total_calls_del', 
    python_callable=del_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'file': 'Total_calls.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
total_calls_31d_sql_del = PythonOperator(
    task_id='total_calls_31d_del', 
    python_callable=del_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'file': 'Total_calls_31d.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
transfer_del = PythonOperator(
    task_id='transfer_del', 
    python_callable=del_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'file': 'transfer.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )

# Блок выполнения SQL запросов.
all_users_sql = MySqlOperator(
    task_id='all_users_sql', 
    sql='/SQL/All_users_to_csv.sql', 
    dag=dag
    )
super_sql = MySqlOperator(
    task_id='super_sql', 
    sql='/SQL/Super_to_csv.sql', 
    dag=dag
    )
total_calls_sql = MySqlOperator(
    task_id='total_calls_sql', 
    sql='/SQL/Total_calls_to_csv.sql', 
    dag=dag
    )
total_calls_31d_sql = MySqlOperator(
    task_id='total_calls_31d_sql', 
    sql='/SQL/Total_calls_31d_to_csv.sql', 
    dag=dag
    )
transfer_sql = MySqlOperator(
    task_id='transfer_sql', 
    sql='/SQL/Transfer_to_csv.sql', 
    dag=dag
    )

# Блок перемещения файлов с сервера MySQL на сервер Airflow.
all_users_transfer = PythonOperator(
    task_id='all_users_transfer', 
    python_callable=transfer_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'to_path': path_to_file_airflow, 'file': 'All_users.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
super_transfer = PythonOperator(
    task_id='super_transfer', 
    python_callable=transfer_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'to_path': path_to_file_airflow, 'file': 'Super.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
total_calls_transfer = PythonOperator(
    task_id='total_calls_transfer', 
    python_callable=transfer_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'to_path': path_to_file_airflow, 'file': 'Total_calls.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
total_calls_31d_sql_transfer = PythonOperator(
    task_id='total_calls_31d_transfer', 
    python_callable=transfer_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'to_path': path_to_file_airflow, 'file': 'Total_calls_31d.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )
transfer_transfer = PythonOperator(
    task_id='transfer_transfer', 
    python_callable=transfer_file, 
    op_kwargs={'from_path': path_to_file_mysql, 'to_path': path_to_file_airflow, 'file': 'transfer.csv', 'db': 'Server_MySQL'}, 
    dag=dag
    )

# Блок преобразования пользователей и супервайзеров.
all_users_clear = PythonOperator(
    task_id='all_users_clear', 
    python_callable=clear_file, 
    op_kwargs={'my_file': f'{path_to_file_airflow}All_users.csv'}, 
    dag=dag
    )
super_clear = PythonOperator(
    task_id='super_clear', 
    python_callable=clear_file, 
    op_kwargs={'my_file': f'{path_to_file_airflow}Super.csv'}, 
    dag=dag
    )

# Блок отправки всех файлов в папку DBS.
all_users_transfer_to_dbs = PythonOperator(
    task_id='all_users_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'All_users.csv', 'db': 'DBS'}, 
    dag=dag
    )
all_users_clear_transfer_to_dbs = PythonOperator(
    task_id='all_users_clear_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'All_users_clear.csv', 'db': 'DBS'}, 
    dag=dag
    )
super_transfer_to_dbs = PythonOperator(
    task_id='super_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'Super.csv', 'db': 'DBS'}, 
    dag=dag
    )
super_clear_transfer_to_dbs = PythonOperator(
    task_id='super_clear_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'Super_clear.csv', 'db': 'DBS'}, 
    dag=dag
    )
total_calls_transfer_to_dbs = PythonOperator(
    task_id='total_calls_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'Total_calls.csv', 'db': 'DBS'}, 
    dag=dag
    )
total_calls_31d_sql_transfer_to_dbs = PythonOperator(
    task_id='total_calls_31d_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'Total_calls_31d.csv', 'db': 'DBS'}, 
    dag=dag
    )
transfer_transfer_to_dbs = PythonOperator(
    task_id='transfer_transfer_to_dbs', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs, 'file': 'transfer.csv', 'db': 'DBS'}, 
    dag=dag
    )

# Два файла нужны для отчета №4.
all_users_clear_transfer_to_dbs_4_rep = PythonOperator(
    task_id='all_users_clear_transfer_to_dbs_4_rep', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs_4_rep, 'file': 'All_users_clear.csv', 'db': 'DBS'}, 
    dag=dag
    )
super_clear_transfer_to_dbs_4_rep = PythonOperator(
    task_id='super_clear_transfer_to_dbs_4_rep', 
    python_callable=transfer_file_to_dbs, 
    op_kwargs={'from_path': path_to_file_airflow, 'to_path': path_to_file_dbs_4_rep, 'file': 'Super_clear.csv', 'db': 'DBS'}, 
    dag=dag
    )

# Отправка уведомления об ошибке в Telegram.
send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='Telegram',
        chat_id='-1001412983860',
        text='Произошла ошибка работы отчета №10.',
        dag=dag,
        # on_failure_callback=True,
        # trigger_rule='all_success'
        trigger_rule='one_failed'
    )

# Блок очередности выполнения задач.
all_users_del >> all_users_sql >> all_users_transfer >> all_users_clear >> [all_users_transfer_to_dbs, all_users_clear_transfer_to_dbs, all_users_clear_transfer_to_dbs_4_rep]
super_del >> super_sql >> super_transfer >> super_clear >> [super_transfer_to_dbs, super_clear_transfer_to_dbs, super_clear_transfer_to_dbs_4_rep]
total_calls_del >> total_calls_sql >> total_calls_transfer >> total_calls_transfer_to_dbs
total_calls_31d_sql_del >> total_calls_31d_sql >> total_calls_31d_sql_transfer >> total_calls_31d_sql_transfer_to_dbs
transfer_del >> transfer_sql >> transfer_transfer >> transfer_transfer_to_dbs
[all_users_transfer_to_dbs, all_users_clear_transfer_to_dbs, all_users_clear_transfer_to_dbs_4_rep, super_transfer_to_dbs, 
super_clear_transfer_to_dbs, super_clear_transfer_to_dbs_4_rep, total_calls_transfer_to_dbs, total_calls_31d_sql_transfer_to_dbs, 
transfer_transfer_to_dbs] >> send_telegram_message
