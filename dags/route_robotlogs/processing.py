#  Функции, подготавляивающие датасеты для загрузки в power bi в цикле по дням

import pandas as pd
import route_robotlogs.all_functions as func
from route_robotlogs.sql_query import sql_query_to_csv
import numpy as np
from datetime import datetime, date


def group_log(path_project_folder, path_sql_log, path_sql_dest, path_sql_city, path_to_robot_log, path_to_route_unique, path_to_route,
              path_to_defenition, file_name_operator):
           

    count = 2
    cloud_name = 'cloud_128'

    for i in range(1, count):

        # Определим дату, за которую будем выгружать данные
        
        date_f = date.today() - pd.Timedelta(days=i)
        print(date_f)

        # Сгенерируем название для файлов на основе этой даты
    
        i_date = '{}_{}_{}.csv'.format(date_f.year, '{0:0>2}'.format(date_f.month), '{0:0>2}'.format(date_f.day))
    
        name_df_step = 'steps/steps_{f}'.format(f=i_date)
        name_df_team = 'teams/teams_{f}'.format(f=i_date)
        name_df_queue = 'queues/queues_{f}'.format(f=i_date)

        file_name_log = 'robot_log_{f}'.format(f=i_date)
        file_name_route = 'route_{f}'.format(f=i_date)
        file_name_route_unique = 'route_unique_{f}'.format(f=i_date)
        file_name_city = 'city_{f}'.format(f=i_date)
        file_name_dest = 'dest_{f}'.format(f=i_date)

        # Загрузим датасеты с шагами, командами и очередями
    
        steps = pd.read_csv(f'{path_to_defenition}{name_df_step}')
        print('___download step', steps.shape[0])
        teams = pd.read_csv(f'{path_to_defenition}{name_df_team}')
        print('___download teams', teams.shape[0])
        queue_project = pd.read_csv(f'{path_to_defenition}{name_df_queue}')
        print('___download queue', queue_project.shape[0])
       
        #  Загружаем датасеты с логами, названными городами и переводами

        sql_query_to_csv(cloud=cloud_name, path_sql_file=path_sql_log, interval=i, path_csv_file=path_to_robot_log, name_csv_file=file_name_log)
        sql_query_to_csv(cloud=cloud_name, path_sql_file=path_sql_city, interval=i, path_csv_file=path_project_folder, name_csv_file=file_name_city)
        sql_query_to_csv(cloud=cloud_name, path_sql_file=path_sql_dest, interval=i, path_csv_file=path_project_folder, name_csv_file=file_name_dest)
        
        df_callcity = pd.read_csv(f'{path_project_folder}{file_name_city}', sep=';')
        df_dest = pd.read_csv(f'{path_project_folder}{file_name_dest}', sep=';')
        df_log = pd.read_csv(f'{path_to_robot_log}{file_name_log}', sep=';')
   
        print('___загрузили датасеты')
        print('___log size ', df_log.shape[0])
        print('___callcity size ', df_callcity.shape[0])
        print('___df_dist size ', df_dest.shape[0])
    
        # Убираем дубликаты разговоров, где названы больше двух городов

        df_callcity = df_callcity.sort_values(['phone', 'queue', 'date']).\
                                    drop_duplicates(subset=['phone', 'queue'], keep='last').reset_index(drop=True)
    
        # Отсортируем строки по дате и телефону

        df_dest = df_dest.sort_values(['date', 'phone']).reset_index(drop=True)

        # Создаем столбец с датой без времени

        df_dest['day'] = df_dest['date'].astype("datetime64[ns]").dt.to_period('D')

        # Создаем столбец с счетчиком звонков за этот день

        df_dest['num_call'] = df_dest.groupby(['day','phone', 'queue']).cumcount() + 1

        # Убираем строки, дублирующие информацию о переводах

        df_dest = df_dest[df_dest['num_call'] == 1] 

        # Заполним пропущенные значения датафрейме логи

        df_log['client_status'] = df_log['client_status'].fillna('Пусто')
        df_log['marker'] = df_log['marker'].astype(str).fillna('Пусто')
        df_log['trunk_id'] = df_log['trunk_id'].fillna(0)
        df_log['real_billsec'] = df_log['real_billsec'].fillna(0)
        print('____тип real_billsec ', df_log['real_billsec'].dtype)
        df_log['real_billsec'] = df_log['real_billsec'].astype(int)
        print('____поменяли тип real_billsec на ', df_log['real_billsec'].dtype)
        print('___что возвращает функция ', func.fill_billsec(0, 7))
        df_log['real_billsec'] = df_log.apply(lambda row: func.fill_billsec(row['real_billsec'], row['billsec']), axis=1)
    
        # Преобразуем типа данных для дальнейшего слияния таблиц

        df_dest['phone'] = df_dest['phone'].astype(str)
        df_dest['destination_queue'] = df_dest['destination_queue'].astype(str)
        df_dest['queue'] = df_dest['queue'].astype(str)

        df_callcity['phone'] = df_callcity['phone'].astype(str)
        df_log['phone'] = df_log['phone'].astype(str)

        # Создадим столбец time

        df_callcity['time'] = (df_callcity['date'].astype("datetime64[ns]") -
                               pd.Timedelta(minutes=180)).dt.to_period('H')
                            # datetime.timedelta(minutes=180)).dt.to_period('H')
        df_log['time'] = df_log['date'].astype("datetime64[ns]").dt.to_period('H')

        # Преобразуем столбцы дат, оставим только дату

        df_dest['date'] = df_dest['date'].astype("datetime64[ns]").dt.to_period("D")
        df_log['date'] = df_log['date'].astype("datetime64[ns]").dt.to_period("D")
        df_callcity['date'] = df_callcity['date'].astype("datetime64[ns]").dt.to_period("D")

        # Объединяем таблицы переводы и логи

        df_log['queue'] = df_log['queue'].astype(str)
    
        print('___перобразовали даные ')
    
        print('___log size ', df_log.shape[0])
        print('___callcity size ', df_callcity.shape[0])
        print('___dist size ', df_dest.shape[0])
    
        df = pd.merge(df_log, df_dest, how = 'left', 
                         right_on =['queue', 'date', 'phone'],
                         left_on = ['queue', 'date', 'phone'])
        
        print('___merge dest ', df.shape[0])
    
        # Объединяем полученную таблицу с таблицей с названными городами

        df_callcity['queue'] = df_callcity['queue'].astype(str)

        df_union = pd.merge(df, df_callcity[~df_callcity.duplicated()][['queue', 'phone', 'time', 'city']], how = 'left', 
                         right_on =['queue', 'phone', 'time'],
                         left_on = ['queue', 'phone', 'time'])
        
        print('___merge city ')
    
        print('___df_unoin size ', df_union.shape[0])
    
        # Убираем ненужные столбцы

        del df_union['time']
        del df_union['day']
        del df_union['num_call']

        del df_callcity
        del df_dest

        print('___delete city dest')

        # Заполняем пустые значения в таблице

        df_union['destination_queue'] = df_union['destination_queue'].fillna('Конец разговора') 
        df_union['city'] = df_union['city'].fillna('Пусто')
            
        # Поменяем типы данных для объедиения в датасет steps

        steps['ochered'] = steps['ochered'].astype(str)
        steps['step'] = steps['step'].astype(str)
        df_union['queue'] = df_union['queue'].astype(str)
        df_union['last_step'] = df_union['last_step'].astype(str)

        df_union['date'] = df_union['date'].astype("datetime64[ns]").dt.to_period("D")
        steps['date'] = steps['date'].astype("datetime64[ns]").dt.to_period("D")

        # Объединим датафреймы, чтобы получить информацию о переводах

        df_union = pd.merge(df_union, steps, how = 'left', 
                         right_on =['step', 'ochered', 'date'],
                         left_on = ['last_step', 'queue', 'date'])
         
        print('___merge step ', df_union.shape[0])

        del steps

        print('___del steps ')

        # Создадим столбец Переводы

        df_union['ochered'] = df_union['ochered'].fillna('0')

        df_union['transfer'] = df_union['ochered'].apply(func.create_dist)
  
        # Убираем ненужные столбцы

        del df_union['type_steps']
  
        df_union['step'] = df_union['step'].fillna(' ')
    
        # Создадим датафрейм с командами и операторами, для определения проектов

        # Загрузим датафрейм с операторами

        operators =  pd.read_csv(f'{path_project_folder}{file_name_operator}')
    
        print('___download operators ')
    
        print('___df_operator size ',operators.shape[0])

        operators = operators.rename(columns = {'id' : 'id_oper'})
                        
        teams['num_team'] = teams['supervisor'].apply(func.find_num)
        operators['num_team'] = operators['supervisor'].apply(func.find_num)

        teams = teams.sort_values(['num_team', 'date']).reset_index(drop=True)
        teams = teams[~teams.duplicated(['num_team', 'date'])].reset_index(drop=True)
    
        # Объединим датафрейм операторы и наш датасет со звонками

        df_union = pd.merge(df_union, operators[['id_oper', 'num_team']], how = 'left',
                                        right_on = 'id_oper', left_on = 'assigned_user_id')
        
        print('___merge operators ')
        print('___df union size ', df_union.shape[0])

        del operators

        print('___delete operators ')

        df_union.loc[df_union['assigned_user_id'] == '1', 'num_team'] = ' '
   
        # Изменим тип данных в столбцу дата для дальнейшего объединения

        teams['date'] = teams['date'].astype('datetime64[ns]').dt.to_period('D')

        # Переименуем столбец для объединения

        teams = teams.rename(columns={'project' : 'p_team'})

        #  Добавим датафрейм с командами

        df_union = pd.merge(df_union, teams[['num_team', 'date', 'p_team']], how = 'left',
                     right_on = ['num_team', 'date'], left_on = ['num_team', 'date'])
        
        print('___merge team ')
    
        print('___df union size ', df_union.shape[0])

        del teams
        print('___delete teams ')


        df_union['p_team'] = df_union['p_team'].fillna('0')

        # queue_project = pd.read_csv(f'{path_project_folder}/{file_name_queue}')

        queue_project = queue_project.rename(columns={'Проект (набирающая очередь)': 'destination_project'})
        queue_project['destination_project'] = queue_project['destination_project'].fillna('DR')
        queue_project['Очередь'] = queue_project['Очередь'].fillna(0).astype('int').astype('str')
        queue_project['date'] = queue_project['date'].astype('str')

        queue_project['RN'] = queue_project.groupby(['Очередь', 'date']).cumcount() + 1
        queue_project = queue_project[queue_project['RN'] == 1][['Очередь', 'destination_project', 'date']]

        print('__change_project')
    
        # Изменим названия и типы данных для слияния таблиц

        queue_project = queue_project.rename(columns = {'Очередь' : 'queue'})
        queue_project['queue'] = queue_project['queue'].astype('str')
        df_union['date'] = df_union['date'].astype(str)
    
        print('___download_queue ')
    
        print('___df queue size ', queue_project.shape[0])


        # Объединим таблицу с проектами и наш датафрейм

        df_union = df_union.merge(queue_project[['queue', 'date', 'destination_project']], how='left',\
                          left_on=['date', 'queue'], right_on=['date', 'queue'])
    
        print('___merge queue ')
    
        print('___df union size ', df_union.shape[0])

        del queue_project
        print('__delete_queue')
    
        df_union['project'] = df_union.apply(lambda row: func.search_project(
                                                row['p_team'],
                                                row['destination_project']), axis=1)

        #  Заменим значения TELE2 в нижнем регистре на верхний

        df_union.loc[df_union['project'] ==  'Tele2', 'project'] = 'TELE2'

        #  УДалим ненужные столбцы

        del df_union['id_oper']
        del df_union['p_team']
        del df_union['num_team']
        del df_union['destination_project']

        # Преобразуем данные в столбце регион для объединения таблиц

        df_union['region_c'] = df_union['region_c'].astype(str)
        df_union['ptv_c'] = df_union['ptv_c'].astype(str)

    
        df_union['quality'] = df_union.apply(lambda row: func.quality(row['ptv_c'], row['region_c']), axis=1)
    
        print('___quality done ')
    
        # Переименуем столбец для более понятного восприятия информации

        df_union = df_union.rename(columns = {'city' : 'call_city'})

        # Выберем только нужные нам столбцы

        new_df = df_union[['phone', 'assigned_user_id', 'date', 'last_step', 'client_status', 'quality',
                   'queue', 'destination_queue', 'route', 'directory', 'real_billsec', 'billsec',
                   'city_c', 'call_city', 'was_repeat', 'trunk_id', 'inbound_call', 'transfer', 'project', 'marker', 'network_provider']]
        

        new_df['real_billsec'] = new_df['real_billsec'].astype('int64')

        # Сгруппируем повторяющиеся строки с одинаковыми маршрутами

        df_group = new_df.groupby(['assigned_user_id', 'date', 'city_c', 'queue', 'destination_queue', 
                            'directory', 'last_step', 'client_status', 'quality', 'was_repeat', 
                            'call_city', 'trunk_id', 'inbound_call', 'transfer', 'project', 'marker',
                            'route', 'network_provider'])[['phone', 'billsec', 'real_billsec']]\
                            .agg({'phone': ['count'], 'billsec' : ['sum'], 'real_billsec' : ['sum']})\
                            .reset_index()

        print('___group done ')  
        
        print('___df group size ', df_group.shape[0])
        
        del df_union
        del new_df

        print('__delete df_union & new df')
        
        df_group.columns = df_group.columns.droplevel(1)
    
        df_group['step_transfer'] = df_group.apply(lambda row: func.func_step(row['destination_queue'], row['last_step']), axis=1)
    
        # Создание столбца с итоговой очередью

        df_group['result'] = df_group.apply(lambda row: func.func_res(row['destination_queue'], row['queue']), axis=1)
   
        # Манипуляции над столбцом маршрут

        df_group['route'] = df_group['route'].apply(func.route_ss).apply(func.to_list).apply(func.del_steps)\
                                      .astype(str).apply(func.route_ss2)  
        
        print('преобразуем столбец удаляем лишнее ')

        # Преобразование столбца с маршрутом обратно в список для дальнейших манипуляций

        df_group['route'] = df_group['route'].apply(func.to_list2)

        # Преобразование столбца маршрут в строку но без пробелов
        
        df_group['route'] = df_group['route'].apply(func.my_str)
    
        # Создание датафрейма с уникальными маршрутами
        print('____манипуляции с столбцом route')

        route_unique = pd.DataFrame(data = df_group['route'].unique(), columns = ['route'])

        # 1
           
        # Создание датасета с уникальными значениями
        # Загружаем предыдущий датафрейм, чтобы дополнить
       
        # route_unique_old = pd.read_csv(f'{path_project_folder}{file_name_unique_route}')
   
        # print('___download old route unique')
        # print('___df old route unique size ', route_unique_old.shape[0])
    
        # Объединим два датасета и удалим дубликаты
    
        #     route_unique_new = pd.concat([route_unique, route_unique_old], ignore_index=True).reset_index(drop=True)  
        #     route_unique_new = route_unique_new.drop_duplicates()
        # route_unique_new = pd.concat([route_unique_old, route_unique]).reset_index(drop=True).drop_duplicates('route')
        
    
        # print('___concat route unique and old')
        # print('___df route unique size ', route_unique_new.shape[0])
            
        # # Запись датафрейма с уникальными маршрутами в файл

        # route_unique_new.to_csv(f'{path_project_folder}{file_name_unique_route}', index=False)
    
        # print('___to save route unique ')
    
        # print('___df route unique size ', route_unique_new.shape[0])





        route_unique.to_csv(f'{path_to_route_unique}{file_name_route_unique}', index=False)




    
        # Создадим датасет с подробными маршруатми
    
        route_unique['route_list'] = route_unique['route'].apply(func.to_list)
        route_unique['test_col'] = np.nan
        
        route_unique['test_col'] = route_unique['route_list'].apply(func.create_liststep)
    
        route_unique = route_unique.explode('test_col').reset_index(drop=True)
    
        del route_unique['route_list']

        for index, row in route_unique.iterrows():
   
            for j, step in enumerate(row['test_col']):
        
                if j == 0:
                    if len(route_unique.columns) == 2:
                
                        route_unique.insert(loc = len(route_unique.columns) , column ='Промежуточный шаг', value = np.nan)
                        route_unique['Промежуточный шаг'][index] = step
                
                
                    else:
                        route_unique['Промежуточный шаг'][index] = step
                else:
                    if len(route_unique.columns) < 3 + j:
                
                        route_unique.insert (loc= len(route_unique.columns) , 
                                         column='Шаг_{}'.format('{0:0>2}'.format(j)), value= np.nan)
                                          
                        route_unique['Шаг_{}'.format('{0:0>2}'.format(j))][index] = step
                    else:
                
                        route_unique['Шаг_{}'.format('{0:0>2}'.format(j))][index] = step
                    
        del route_unique['test_col']
    
        print('___create route')
    
        # Добавим столбцы до шаг_24
    
        if len(route_unique.columns) < 33:
            for i in range(len(route_unique.columns)+1, 34):
                route_unique.insert(loc= len(route_unique.columns) , column='Шаг_{}'.format(i-2), value = np.nan)

        route_unique = route_unique.fillna('Пусто')
        route_unique = route_unique.astype(str)
            
        print('___add columns route')
        print('___df routesize ', route_unique.shape[0])





        # 2
        # Загружаем предыдущий датафрейм, чтобы дополнить
    
        # route_old = pd.read_csv(f'{path_project_folder}{file_name_route}')
    
        # print('___download old route')
        # print('___df кол-во столбцов ', len(route_old.columns))
        # print('___df old route size ', route_old.shape[0])
    
        # # Объединим два датасета и удалим дубликаты

        # route = pd.concat([route_old, route_unique]).reset_index(drop=True).drop_duplicates(['route', 'Промежуточный шаг', 'Шаг_01'])

        # print('___concat route and old')
        # print('___df кол-во столбцов ', len(route.columns))
        # print('___df route size ', route.shape[0])
            
        # # Запись датафрейма с подробными маршрутами в файл

        # route.to_csv(f'{path_project_folder}{file_name_route}', index=False)
    
        # print('___to save route ')
        # print('___df route size ', route.shape[0])






        # Запись датафрейма с подробными маршрутами в файл

        route_unique.to_csv(f'{path_to_route}{file_name_route}', index=False)
    
        print('___to save route ')
    
        print('___df routesize ', route_unique.shape[0])

        del route_unique

        print('___delete_route_unique')
    
        # Меняем порядок столбцов в датафрейме

        df_group = df_group[['date', 'call_city', 'project', 'queue', 'destination_queue', 'result',
                     'directory', 'route', 'step_transfer', 'last_step',  'client_status', 'billsec', 'real_billsec', 
                     'was_repeat', 'assigned_user_id', 'quality', 'city_c', 
                     'trunk_id', 'phone', 'inbound_call', 'transfer', 'marker', 'network_provider']]
        # Переименовываем названия столбцов

        df_group = df_group.rename(columns = {'date' : 'Дата', 'call_city' : 'Названный город', 
                'project' : 'Проект', 'queue' : 'Очередь', 
                'destination_queue' : 'Очередь перевода', 'result' : 'Итоговая Очередь', 'directory' : 'Директория',
                'route' : 'Маршрут' , 'step_transfer' : 'Шаг перевода', 'last_step' : 'Последний шаг',
                'client_status' : 'Статус клиента', 'billsec' : 'Время распознавания',
                'real_billsec' : 'Время разговора', 'was_repeat' : 'Была ПТВ', 'assigned_user_id' : 'Оператор',
                'quality' : 'Качество', 'city_c' : 'Город код', 'trunk_id' : 'Код телефонии',
                'phone' : 'Количество id', 'inbound_call' : 'Входящие звонок', 'transfer' : 'Перевод',
                'marker' : 'Маркер', 'network_provider' : 'Оператор связи'})

        # Меняем тип данных на целое число

        df_group['Код телефонии'] = df_group['Код телефонии'].astype('int64')

        # Записываем в файл

        df_group.to_csv(f'{path_to_robot_log}{file_name_log}', index= False)

        del df_group

        print('___delete df_group')        



def processing_quality(file_name_quality):

    df_quality = pd.read_csv(file_name_quality)

    # Заполним значение None пустыми строками в столбце call city и Качество города

    df_quality['Качество города'] = df_quality['Качество города'].fillna('Пусто')

    # Убираем лишние скобки в столбце Качество города

    df_quality['Качество города'] = df_quality['Качество города'].apply(func.del_staple)

    df_quality.to_csv(file_name_quality, index = False)

    print('___save_quality')


def processing_cities(file_name_cities):

    df_cities = pd.read_csv(file_name_cities)

    # Преобразуем столбец город в строку

    df_cities['city_c'] = df_cities['city_c'].astype(str)

    # Заполняем пропущенные значения 'Пусто'

    df_cities['Город'] = df_cities['Город'].fillna('Пусто')
    df_cities['Область'] = df_cities['Область'].fillna('Пусто')

    # Убираем лишние скобки в названиях городов

    df_cities['Город'] = df_cities['Город'].apply(func.del_staple)
    df_cities['Область'] = df_cities['Область'].apply(func.del_staple)

    df_cities[['city_c', 'Город', 'Область']].to_csv(file_name_cities, index=False)

    print('___save_cities')


def processing_hours(file_name_hours):

    hours = pd.read_csv(file_name_hours)
    # Меняем тип данных 

    hours['worktime'] = hours['worktime'].fillna(0).astype('int64')

    # Записываем в файл

    hours.to_csv(file_name_hours, index=False)

def xlsx_to_csv(file_xlsx, file_csv, sheet_name):

    # Загружаем датасет с файла excel

    df = pd.read_excel(file_xlsx, sheet_name=sheet_name)
    
    # Сохраняем файл в папку с проектом

    df.to_csv(file_csv, index=False)

# Удаляем лишний ноль в столбце очереди таблицы групировк очередей

def convert_ro(path_to_folder, file_name):

    # Загружаем датасет с типами РО

    df = pd.read_csv(f'{path_to_folder}{file_name}')

    print(df.columns)

    # Преобразуем значение

    df['Очередь'] = df['Очередь'].astype(int)

    # Сохраняем с типами РО


    df.to_csv(f'{path_to_folder}{file_name}', index = False)


def create_conv(path_to_folder, file_name_request, file_name_conv):

    # Загрузим датафрейм заявки

    request = pd.read_csv(f'{path_to_folder}{file_name_request}')

    col_list = ['queue', 'destination_queue', 'team', 'konva', 'vsego', 'marker', 'last_step', 'trunk_id', 'inbound_call']

    for col in col_list:
        request.loc[request[col] == ' ', [col]] = 0
        request[col] = request[col].fillna(0)
        request[col] = request[col].astype(int).astype(str)
    
    request.to_csv(f'{path_to_folder}{file_name_request}')
    # Отбираем только нужные нам столбцы

    df_request = request[['proect', 'uid', 'city_c', 'organization', 'team']]
    df_request = df_request.drop_duplicates(subset=['proect', 'uid', 'city_c', 'organization', 'team'])

    
    # Добавляем к заявкам столбец с датами

    df_request['date'] = df_request['city_c'].apply(func.fill_date)

    # Взрываем датасет, оставляя нужные нам колонки

    print('___create_conv')

    df_conv = df_request.explode('date').reset_index(drop = True)

    # Создание столбцов с периодом дат

    df_conv['date_40'] = df_conv.apply(lambda row: func.date_end(row['date'], row['proect']), axis=1)
    df_conv['date_60'] = df_conv.apply(lambda row: func.date_start(row['date'], row['proect']), axis=1)

    # Преобразуем столбец team в текстовый формат

    df_conv['team'] = df_conv['team'].fillna(0)

    df_conv['team'] = df_conv['team'].astype(int).astype(str)

    # Записываем в файл

    df_conv.to_csv(f'{path_to_folder}/{file_name_conv}', index=False)

    print('___save_conv')














 