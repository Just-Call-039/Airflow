def project_steps():
    import pandas as pd
    import datetime
    import MySQLdb

    host128 = "192.168.1.128"
    host = host128
    Con = MySQLdb.Connect(host=host, user="base_dep_slave", passwd="QxPGHGdzCLao", db="suitecrm",
                        charset='utf8')

    sql_steps = '/root/airflow/dags/project_defenition/steps.sql'
    to_save = '/root/airflow/dags/project_defenition/projects/steps/steps_{}.csv'
    to_save_steps = to_save.format(datetime.datetime.now().strftime("%Y_%m_%d"))

    print('Выгружаем данные steps')
    sql_steps = open(sql_steps, 'r')
    sql_steps = sql_steps.read()
    steps = pd.read_sql_query(sql_steps, Con)
    steps['date'] = datetime.datetime.now().strftime('%Y-%m-%d')
    steps.to_csv(to_save_steps, index=False)


