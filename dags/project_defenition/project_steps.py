def project_steps():
    import pandas as pd
    import datetime
    import MySQLdb

    host128 = "192.168.1.183"
    host = host128
    Con = MySQLdb.Connect(host=host, user="base_dep_slave", passwd="IyHBh9mDBdpg", db="suitecrm",
                        charset='utf8')

    sql_steps = '/root/airflow/dags/project_defenition/steps.sql'
    to_save = '/root/airflow/dags/project_defenition/projects/steps/steps_{}.csv'
    to_save_steps = to_save.format(datetime.datetime.now().strftime("%Y_%m_%d"))

    print('Выгружаем данные steps')
    sql_steps = open(sql_steps, 'r')
    sql_steps = sql_steps.read()
    steps = pd.read_sql_query(sql_steps, Con)
    current_date = datetime.datetime.now()
    yesterday_date = current_date - datetime.timedelta(days=0)
    steps['date'] = yesterday_date.strftime('%Y-%m-%d')
    steps = steps[['step','ochered','date','type_steps']]
    steps.to_csv(to_save_steps, index=False)


