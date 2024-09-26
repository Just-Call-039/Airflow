# from airflow import DAG
# from airflow.providers.papermill.operators.papermill import PapermillOperator
# from datetime import datetime, timedelta

# # Определение аргументов по умолчанию для DAG
# default_args = {
#     'owner': 'root',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 8, 24),  # Установите дату начала
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Создание DAG
# with DAG(
#     'run_jupyter_script_trikolor',
#     default_args=default_args,
#     description='DAG для запуска Jupyter Notebook',
#     schedule_interval='0 18 * * *',  # Ежедневно в 19:00 по Москве (MSK)
#     catchup=False,
#     timezone='Europe/Kaliningrad',
# ) as dag:
#     # Операция для запуска Jupyter Notebook
#     run_notebook = PapermillOperator(
#         task_id='run_script_trikolor',
#         input_nb='/root/airflow/dags/script_survey_trikolor/script_aitflow - рабочий.ipynb',  # Путь к вашему .ipynb файлу
#         output_nb='/root/airflow/dags/script_survey_trikolor/notebook-{{ execution_date }}.ipynb',  # Где сохранить результат
#         parameters={"execution_date": "{{ execution_date }}"},
#     )

#     run_notebook
