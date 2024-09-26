
# Все функции для обработки данных, используемых в проекте

import pandas as pd
import datetime

# Заполнение столбца время звонка значениями из столбца время распознавания:

def fill_billsec( realbillsec, billsec):
    
    if realbillsec == 0:
        return billsec
    else:
        return realbillsec
    
# Создание столбца Перевод

def create_dist(x):
    if x == '0':
        return 0
    else:
        return 1

# Поиск числа в строке для извлечения команды

def find_num(line):
    num = ''
    if type(line) is str:
        for letter in line:
            if num != '':
                if letter != ' ':
                    if letter.isdigit():
                        num += letter
                    else:
                        return num
            else:
                if letter.isdigit():
                        num += letter


# Определение к какому проекту относится звонок 

def search_project(p_team, project_d):
      
    if (p_team == '0'):
        return project_d
    elif  (p_team == 'Вход'):
        return project_d
    elif  (p_team == 'ОД'):
        return project_d
    else:
        return p_team

# Функция замены нужных словесных шагов на числа

def route_ss(x):
    return x.replace('ПУСТЫ', '7001').replace('N', '7002').replace('СОГЛА', '7003').\
                replace('ДА СО', '7003').replace('ОТКАЗ', '7004').replace('НЕТ К', '7005').\
                    replace('УСТРА', '7006')

# Обратная замена этих шагов на названия

def route_ss2(x):
    return x.replace('7001', 'Пустышка').replace('7002', 'TimeOut').replace('7003', 'Согласие').\
        replace('7004', 'Отказ').replace('7005', 'Отказ2').replace('7006', 'Устраивает')

# Преобразование столбца в список

def to_list(x):
    return x.split(',')

# Преобразование столбца в список, c другой пунктуацией

def to_list2(x):
    x = x[2:]
    x = x[:-2]
    return x.split("', '")

# Удаление шагов, состоящих из букв

def del_steps(x):
    new_list = []
    for i in x:
        if i.isdigit() == True:
            new_list.append(i)
    return new_list

# Определение, какой разметке соответсвует звонок

def quality(ptv, region):
    
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

# Удаление скобок в начале и конце строки
 
def del_staple(x):
    
    if x != 'Пусто':
        x = x[1:]
        x = x[:-1]
    
    return x

# Создание столбца шаг перевода

def func_step(dest, last_step):
    
    if dest == 'Конец разговора':
        return dest
    else:
        return last_step

# Создание столбца с итоговой очередью

def func_res(dest, dialog):
    
    if dest == 'Конец разговора':
        return dialog
    else:
        return dest

# Функции для создания таблиц с маршрутами    
# Преобразование столбца маршрут в строку но без пробелов

def my_str(route_list):
    route_str = ''
    for i in route_list:
        route_str = route_str + i
        route_str = route_str + ','
    return route_str[:-1]

# Создание столбца со списком подробных маршрутов

def create_liststep(route):
    my_list = []
    for j in range(len(route)):
        my_list.append(route[j:])
    return my_list


# Функции для создания таблицы Конверсия
# Создание столбцов с периодом дат
 
def date_end(date, project):

    if date < datetime.date.today() - datetime.timedelta(days=61):
        if project in ['GULFSTREAM', 'GULFSTREAM LIDS', 'Delta', 'Delta LIDS']:
            if date > datetime.date.today() - datetime.timedelta(days=81):
                return date - datetime.timedelta(days=22)
            else:
                return date
        else:
            return date
    else:
        return date - datetime.timedelta(days=22)

def date_start(date, project):
    
    if date < datetime.date.today() - datetime.timedelta(days=61):
        if project in ['GULFSTREAM', 'GULFSTREAM LIDS', 'Delta', 'Delta LIDS']:
            if date > datetime.date.today() - datetime.timedelta(days=81):
                return date - datetime.timedelta(days=81)
            else:
                return date
        else:
            return date
    else:
        return date - datetime.timedelta(days=61)

# def date_20(date):
#     if date > datetime.date.today() - datetime.timedelta(days=40):
#         return date - datetime.timedelta(days=20)
#     else:
#         return date

# def date_40(date):
    
#     if date >= datetime.date.today() - datetime.timedelta(days=40):
#         return date - datetime.timedelta(days=40)
#     else:
#         return date
    
def fill_date(date):

    # Создаем датасет с датами

    df_calendar = pd.DataFrame(pd.date_range(start = "2024-06-01", end = "2024-08-31"),
                                        # start=datetime.date.today() - datetime.timedelta(days=60),
                                        # end=datetime.date.today() + datetime.timedelta(days=1)),
                                        columns = ['date'])
    

    return list(df_calendar['date'])

    
    
