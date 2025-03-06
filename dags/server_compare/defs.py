import pandas as pd
import pymysql
from commons_sawa.connect_db import connect_db

# Функция для извлечения хоста, логина, пароля.
# Необходимо передать файл с соответствующим наименованием.
# Maria_db, 72, Combat, Click, Server_MySQL, DBS, cloud_117, cloud_128.

# Загрузка данных из robot_log
    
def get_data_mysql(path_sql_file, cloud, date_i, path_to_file, file_name):

    print(f'start download {file_name} per : {date_i}')
    
    # Создадим запрос с текущей датой
    sql_request = open(path_sql_file).read().format(str(date_i=date_i))
    
    print('try read file cloud ', cloud)

    # Достанем данные для подключения
    host, user, password = connect_db(cloud)

    print('try connection')

    # Создаем подключение
    my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                 db="suitecrm",
                                 charset='utf8')
    
    #  Загружаем данные в датафрейм
    df = pd.read_sql_query(sql_request, my_connect)
    my_connect.close()

    print(f'download {file_name} - size {df.shape[0]}')

    # Сохраняем данные в csv
    df.to_csv(f'{path_to_file}{file_name}', index = False)

    print('save to csv success')

# Заполнение пустых значений и пробелов нулем

def fillnan_my(x):
    if (x == '') | (x == ' '):
        return '0'
    else:
        return x

# Определение статуса звонка "Перевелись"

def define_perevod_done(perevod, status, operator):
    if (perevod == 1)  & (status in ["MeetingWait", "CallWait", "refusing"]) & (operator not in ['0', '1', '', ' ']):
        return 1
    else:
        return 0
    
# Определение была ли заявка на звонке 

def define_request(status, perevod_done):
    if (status == "MeetingWait") & (perevod_done == 1):
        return 1
    else:
        return 0
    

    
def define_project(project_dialog, project_user, user):
    
    if user == '1':
        return project_dialog
    else:
        return project_user
    
def define_quality(ptv, region):
    
    ptv_nasha = ['^5^', '^6^', '^3^', '^10^', '^11^', '^19^', ]
    ptv_ne_nasha = ['^5_15^', '^5_16^', '^5_17^', '5_18^', '^5_19^', '^5_20^', '^5_21^',
                '^6_15^', '^6_16^', '^6_17^', '6_18^', '^6_19^', '^6_20^', '^6_21^',
                '^3_15^', '^3_16^', '^3_17^', '3_18^', '^3_19^', '^3_20^', '^3_21^',
                '^10_15^', '^10_16^', '^10_17^', '10_18^', '^10_19^', '^10_20^', '^10_21^',
                '^11_15^', '^11_16^', '^11_17^', '11_18^', '^11_19^', '^11_20^', '^11_21^',
                '^19_15^', '^19_16^', '^19_17^', '19_18^', '^19_19^', '^19_20^', '^19_21^']

    if any(w in ptv for w in ptv_nasha):
        return 'ptv_1'
    elif any(w in ptv for w in ptv_ne_nasha):
        return 'ptv_2'
    else:
        return region
    
def delete_staple(x):

    if x != 'Пусто':
        x = x[1:]
        x = x[:-1]    
    return x



def proccessing_quality(df_quality):

    # Заполним значение None пустыми строками в столбце call city и Качество города

    df_quality['Качество города'] = df_quality['Качество города'].fillna('Пусто')

    # Убираем лишние скобки в столбце Качество города

    df_quality['Качество города'] = df_quality['Качество города'].apply(del_staple)

def set_uniqueid(id, server):
    return str(id) + '.' + str(server)