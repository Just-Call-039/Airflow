# import datetime
# import pendulum
# from datetime import timedelta

# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator

# from inbound_report import download_date_mysql, merge_df, save_result_to_click, download_date_clickhouse
# from route_robotlogs.clear_folder import clear_folder
# from commons_liza import dbs

# default_args = {
#     'owner': 'Kunina Elizaveta',
#     'email': 'kunina.elisaveta@gmail.com',
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 3,
#     'retry_delay': timedelta(minutes=3)
#     }

# dag = DAG(
#     dag_id='inbound_to_click',
#     schedule_interval='20 4 * * *',
#     start_date=pendulum.datetime(2023, 7, 25, tz='Europe/Kaliningrad'),
#     catchup=False,
#     default_args=default_args
#     )

# date_i = datetime.date.today() - datetime.timedelta(days=1)
# year = date_i.year
# month = date_i.month
# day = date_i.day

# # date_before = date_i - datetime.timedelta(days=1)


# # cloud_truba = ['Kuzmenko', 'KZY26KpynLWQORNzkDlI5lu3ue7mtyKj', '192.168.1.40', 'asteriskcdrdb']
# cloud_182_call = ['base_dep_slave', 'IyHBh9mDBdpg','192.168.1.182', 'suitecrm']
# # cloud_182_robot = ['base_dep_slave', 'IyHBh9mDBdpg','192.168.1.182', 'suitecrm_robot']

# # # Путь к файлам проекта

# file_path = '/root/airflow/dags/inbound_report/Files/inboun_arhive'

# # # Путь к шагам

# # step_path = '/root/airflow/dags/project_defenition/projects/steps/'

# # # Пути к sql запросам

# # truba_sql_path = '/root/airflow/dags/inbound_report/SQL/get_truba.sql'
# # robot_sql_path = '/root/airflow/dags/inbound_report/SQL/get_robotlog.sql'
# # astin_sql_path = '/root/airflow/dags/inbound_report/SQL/get_astin.sql'
# inbound_sql_path = '/root/airflow/dags/inbound_report/SQL/get_inbound.sql'
# # call_sql_path = '/root/airflow/dags/inbound_report/SQL/get_call.sql'
# # request_sql_path = '/root/airflow/dags/inbound_report/SQL/get_request.sql'

# # # Путь к папке dbs

# # inbound_dbs_path = f'scripts fsp\Current Files\inbound_report\inbound_call\inbound_call_{year:02}_{month:02}_{day:02}.csv'
# # result_dbs_path = f'scripts fsp\Current Files\inbound_report\inbound_sheme\inbound_sheme_{year:02}_{month:02}_{day:02}.csv'

# # # Названия файлов

# # truba_csv = f'truba_{year:02}_{month:02}_{day:02}.csv'
# # astin_csv = f'astin_{year:02}_{month:02}_{day:02}.csv'
# inbound_csv = f'inbound_call_{year:02}_{month:02}_{day:02}.csv'
# # call_csv = f'call_{year:02}_{month:02}_{day:02}.csv'
# # robot_csv = f'robot_{year:02}_{month:02}_{day:02}.csv'
# # step_csv = f'steps_{year:02}_{month:02}_{day:02}.csv'

# # inbound_truba_csv = f'inbound_truba_{year:02}_{month:02}_{day:02}.csv'
# # astin_truba_csv = f'astin_truba_{year:02}_{month:02}_{day:02}.csv'
# # robot_truba_csv = f'robot_truba_{year:02}_{month:02}_{day:02}.csv'
# # call_truba_csv = f'call_truba_{year:02}_{month:02}_{day:02}.csv'
# # request_csv = 'request.csv'




# get_inbound = PythonOperator(
#     task_id = 'get_inbound',
#     python_callable = download_date_mysql.get_data,
#     op_kwargs = {'sql_download' : inbound_sql_path,
#                 'cloud' : cloud_182_call, 
#                 'date_i' :  str(date_i),
#                 'file_path' : f'{file_path}{inbound_csv}'
#                 },
#     dag = dag
#     )      

# # get_astin = PythonOperator(
# #     task_id = 'get_astin',
# #     python_callable = download_date_clickhouse.get_data_ch,
# #     op_kwargs = {'sql_download' : astin_sql_path,
# #                  'date_before' : date_before,
# #                  'date_i' : date_i,
# #                  'file_path' : f'{file_path}{astin_csv}'
# #                 },
# #     dag = dag
# #     )   

# # get_call = PythonOperator(
# #     task_id = 'get_call',
# #     python_callable = download_date_mysql.get_data,
# #     op_kwargs = {'sql_download' : call_sql_path,
# #                 'cloud' : cloud_182_call, 
# #                 'date_i' :  str(date_i),
# #                 'file_path' : f'{file_path}{call_csv}'
# #                 },
# #     dag = dag
# #     )  

# # get_request = PythonOperator(
# #     task_id = 'get_request',
# #     python_callable = download_date_mysql.get_data,
# #     op_kwargs = {'sql_download' : request_sql_path,
# #                 'cloud' : cloud_182_call, 
# #                 'date_i' :  str(date_i),
# #                 'file_path' : f'{file_path}{request_csv}'
# #                 },
# #     dag = dag
# #     ) 


# # union_with_inbound = PythonOperator(
# #     task_id = 'union_with_inbound',
# #     python_callable = merge_df.union_with_inbound,
# #     op_kwargs = {'truba_path' : f'{file_path}{truba_csv}',
# #                 'inbound_path' : f'{file_path}{inbound_csv}', 
                
# #                 'type_dict' : type_dict,
# #                 'result_path' :f'{file_path}{inbound_truba_csv}'
# #                 },
# #     dag = dag
# #     ) 

# # download_exit_dict = PythonOperator(
# #     task_id = 'download_exit_dict',
# #     python_callable = merge_df.union_exit_dict,
# #     op_kwargs = {
# #                 'start_path' : f'{file_path}{inbound_truba_csv}',
# #                 'result_path' : f'{file_path}{inbound_truba_csv}',
# #                 'type_dict' : type_dict
# #                 },
# #     dag = dag
# #     )  

# # union_astin_df = PythonOperator(
# #     task_id = 'union_astin_df',
# #     python_callable = merge_df.union_with_astin,
# #     op_kwargs = {'truba_path' : f'{file_path}{inbound_truba_csv}',
# #                 'astin_path' : f'{file_path}{astin_csv}', 
# #                 'type_dict' : type_dict,
# #                 'result_path' : f'{file_path}{astin_truba_csv}'
                
# #                 },
# #     dag = dag
# #     ) 


# # union_robot_df = PythonOperator(
# #     task_id = 'union_robot_df',
# #     python_callable = merge_df.union_robot_df,
# #     op_kwargs = {'start_path' : f'{file_path}{astin_truba_csv}',
# #                 'robot_path' : f'{file_path}{robot_csv}', 
# #                 'step_path' :f'{step_path}{step_csv}',
# #                 'result_path' : f'{file_path}{robot_truba_csv}',
# #                 'type_dict' : type_dict
# #                 },
# #     dag = dag
# #     )        

       

# # union_call_df = PythonOperator(
# #     task_id = 'union_call_df',
# #     python_callable = merge_df.union_call_df,
# #     op_kwargs = {'start_path' : f'{file_path}{robot_truba_csv}',
# #                 'call_path' : f'{file_path}{call_csv}', 
# #                 'request_path' : f'{file_path}{request_csv}', 
# #                 'result_path' : f'{file_path}{call_truba_csv}',
# #                 'type_dict' : type_dict,
# #                 'date_i' : date_i
# #                 },
# #     dag = dag
# #     )   

  

# # save_result_click = PythonOperator(
# #     task_id = 'save_result_click',
# #     python_callable = save_result_to_click.save_data,
# #     op_kwargs = {
# #                 'result_path' : f'{file_path}{call_truba_csv}',
# #                 'type_dict' : type_dict
# #                 },
# #     dag = dag
# #     )

# # save_request_click = PythonOperator(
# #     task_id = 'save_request_click',
# #     python_callable = save_result_to_click.save_request,
# #     op_kwargs = {
# #                 'result_path' : f'{file_path}{request_csv}'
# #                 },
# #     dag = dag
# #     )

# # save_inbound_dbs = PythonOperator(
# #     task_id = 'save_inbound_dbs',
# #     python_callable = dbs.save_file_to_dbs,
# #     op_kwargs = {
# #                 'path_from' : f'{file_path}{inbound_csv}',
# #                 'path_to' : inbound_dbs_path
# #                 },
# #     dag = dag
# #     )

# # save_result_dbs = PythonOperator(
# #     task_id = 'save_result_dbs',
# #     python_callable = dbs.save_file_to_dbs,
# #     op_kwargs = {
# #                 'path_from' : f'{file_path}{call_truba_csv}',
# #                 'path_to' : result_dbs_path
# #                 },
# #     dag = dag
# #     )



# # clear_folders = PythonOperator(
# #     task_id='clear_folders', 
# #     python_callable=clear_folder, 
# #     op_kwargs={'folder': file_path}, 
# #     dag=dag
# #     )

    
# # [get_truba, get_robotlog, get_inbound, get_call, get_request] >>\
# #       union_with_inbound >>  download_exit_dict >> union_astin_df >> union_robot_df >> union_call_df \
# #       >> save_result_click >> save_request_click >> [save_inbound_dbs, save_result_dbs]  >> clear_folders