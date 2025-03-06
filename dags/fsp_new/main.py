
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from fsp_new import trafic, request


default_args = {
    'owner': 'Kunina Elizaveta',
    'email': 'kunina.elisaveta@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=60)
    }

dag = DAG(
    dag_id='fsp_new',
    schedule_interval='20 3 * * *',
    start_date=pendulum.datetime(2023, 7, 25, tz='Europe/Kaliningrad'),
    catchup=False,
    default_args=default_args
    )


cloud = ['base_dep_slave', 'IyHBh9mDBdpg', '192.168.1.182', 'suitecrm']

n = 1
days = 1
# project folder
trafic_folder_path = '/root/airflow/dags/fsp_new/files/trafic'
marker_path = '/root/airflow/dags/fsp_new/files/Маркера.csv'
trafic_path = '/root/airflow/dags/fsp_new/files/trafic/Трафик_{}.csv'
konva_path = '/root/airflow/dags/fsp_new/files/Заявки_номера.csv'
konva_group_path = '/root/airflow/dags/fsp_new/files/Заявки.csv'

# dbs
trafic_dbs_path = 'scripts fsp\Current Files\ФСП\Трафик\Трафик_{}.csv'
to_dbs_konva = 'scripts fsp\Current Files\Заявки_номера.csv'
to_dbs_konva_group = 'scripts fsp\Current Files\Заявки.csv'

# sql
marker_dbs_path ='scripts fsp\Current Files\ФСП\Трафик_airflow\Маркера.csv'
trafic_sql = '/root/airflow/dags/fsp_new/SQL/trafic.sql'
team_sql = '/root/airflow/dags/fsp_new/SQL/team.sql'
steps_sql = '/root/airflow/dags/fsp_new/SQL/steps.sql'
dialogi_sql = '/root/airflow/dags/fsp_new/SQL/dialogi.sql'
voronka_sql = '/root/airflow/dags/fsp_new/SQL/voronka.sql'
request_sql = '/root/airflow/dags/fsp_new/SQL/request.sql'

trafic_load = PythonOperator(
    task_id = 'trafic',
    python_callable = trafic.get_trafic,
    op_kwargs = {
                'trafic_sql' : trafic_sql,
                'cloud' : cloud,
                'trafic_path' : trafic_path,
                'trafic_dbs_path' : trafic_dbs_path,
                'marker_dbs_path' : marker_dbs_path,
                'team_sql' : team_sql,
                'n' : n,
                'days' : days
                },
    dag = dag
    )

marker_load = PythonOperator(
    task_id = 'marker',
    python_callable = trafic.get_marker,
    op_kwargs = {
                'marker_path' : marker_path,
                'trafic_folder_path' : trafic_folder_path,
                'marker_dbs_path' : marker_dbs_path
                },
    dag = dag
    )

request_load = PythonOperator(
    task_id = 'request',
    python_callable = request.get_request_data,
    op_kwargs = {
                'request_sql' : request_sql,
                'cloud' : cloud,
                'steps_sql' : steps_sql,
                'dialogi_sql' : dialogi_sql,
                'team_sql' : team_sql,
                'voronka_sql' : voronka_sql,
                'konva_path' : konva_path,
                'konva_group_path' : konva_group_path,
                'to_dbs_konva' : to_dbs_konva,
                'to_dbs_konva_group' : to_dbs_konva_group
                },
    dag = dag
    )

             


[trafic_load, request_load] >> marker_load
