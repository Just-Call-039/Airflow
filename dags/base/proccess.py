
import pandas as pd
from datetime import datetime
import pymysql
import math
from time import sleep
from base.defs import download_data, fillnan_my, network_provider_c, phone, region_c, download_to_click, delete_ch, del_staple
from commons_sawa.connect_db import connect_db


# Выгрузка данных с сервера по частям
# start и end - начало и конец лимита выгрузки с сервера
# limit_list лимиты для загрузки 

def data_load(start, end, cloud, sql_file, path_city, dict_project, source_list, step_list, table_name, table_name_ch, sql_download, timeout):

    # Чистим таблицу в Clickhouse со старыми записями

    delete_ch(table_name_ch)

    # Загружаем датасет с городами, чтобы подтянуть названия городов и областей

    print('____download df city')

    df_city = pd.read_csv(path_city).fillna('Пусто')   

    # Определим количество загрузок
      
    try: 
        host, user, password = connect_db(cloud)
        my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                    db="suitecrm",
                                    charset='cp866')
          
        sql_query_size = '''SELECT COUNT(phone_work) FROM suitecrm.{}'''.format(table_name)

        df_size = pd.read_sql_query(sql_query_size, my_connect)['COUNT(phone_work)']

        count_repeat = math.ceil(df_size - start / end)

        print('count_repeat ', count_repeat)
           
    except (EOFError):
        print('Не удалось выгрузить данные')
  
    # Запускаем цикл, в котором будем по кускам загружать данные из базы, обрабатывать их и отправлять на Clickhouse

    for i in range(0, count_repeat):

        print('Download ', i)

        # Подключаемся к серверу
        try: 
            host, user, password = connect_db(cloud)
            my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                                        db="suitecrm",
                                        charset='cp866')

            my_query = open(sql_file).read()
            print('Отправляем запрос')

            chunk = pd.read_sql_query(my_query.format(start, end), my_connect)
               
            print(f'Rows count = {chunk.shape[0]}')
            
            if chunk.shape[0] == 0:
                break
            
        except (EOFError):
            print('Не удалось выгрузить данные')

        # Заполняем пустые значения и пробелы в полях с кодом города и областей нулем

        chunk['town_c'] = chunk['town_c'].fillna('0').apply(fillnan_my).astype('Int64')
        chunk['city_c'] = chunk['city_c'].fillna('0').apply(fillnan_my).astype('Int64')

        # Мерджим Датафрейм с датасетом city, достаем города
        
        chunk = chunk.merge(df_city[['city_c', 'Город']], how='left', on = 'city_c')

        # Мерджим датафрейм с датасетом city, достаем области
        df_town = df_city.drop_duplicates('town_c')
        chunk = chunk.merge(df_town[['town_c', 'Область']], how='left', on = 'town_c')
        print('Размер датафрейма', chunk.shape[0])

        # Удаляем столбцы с кодами обдластей и городов
        del chunk['town_c']
        del chunk['city_c']

    # Переименовываем поля со значениями область и города

        chunk = chunk.rename(columns = {'Область' : 'town_c', 'Город' : 'city_c'})

        print(' __quality_defination')

        # Создаем поля с качестов базы по проектам со значениями 0 и 1 

        list_ptv = ('^10^', '^11^', '^19^', '^3^', '^5^', '^6^', '^14^', '^287^', '^211^')
        list_project = ('bln','mts','nbn','dom','rtk','ttk', '2com', 'tele2', 'tat')

        for i, col in enumerate(list_project):
            
            column = col + '_nasha'   
            chunk[column] = chunk['ptv_c'].apply(lambda x: 1 if list_ptv[i] in str(x) else 0)

        # Создаем поле со значение ptv_n 
        print(' ___ptv_n defination')
        chunk['ptv_n'] = chunk['ptv_c'].astype('str').apply(lambda x: 1 if any(w in x for w in ['^n^']) else 0)
        
        # Хаполняем в строковых полях пустые значения
        col_list_str = ['phone_work', 'stoplist_c', 'ptv_c', 'region_c',
                        'network_provider_c', 'base_source_c', 'priority1', 'priority2',
                        'last_call', 'city_c', 'town_c']
        for col in col_list_str:
            chunk[col] = chunk[col].fillna('Пусто')

        
        # Определяем поле с оператором связи

        chunk['network_provider_c'] = chunk['network_provider_c'].astype('str').apply(lambda x: network_provider_c(x))

        # Определяем поле качество базы

        chunk['region_c'] = chunk['region_c'].astype('str').apply(lambda x: region_c(x))
        chunk['region_c'] = chunk['region_c'].fillna('Пусто')

        # Определяем поле мобильный/немобильный телефон

        chunk['phone'] = chunk['phone_work'].astype('str').apply(lambda x: phone(x))

        # Заполняем пустые значения в поле последний звонок и приводим к нужному формату

        chunk['last_call'] = chunk['last_call'].astype('str').apply(lambda x: '1970-01-01' if x in {'0','0000-00-00', 'Пусто'} else x).\
                                                              apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        
        # Группируем датасет 
        print(' _group chunk')

        df_group = chunk.groupby(['region_c','network_provider_c', 'base_source_c', 'town_c', 
                                  'city_c', 'ntv_ptv', 'step_c', 'priority1', 'priority2',
                                  'last_call', 'bln_nasha', 'mts_nasha', 'nbn_nasha', 'dom_nasha',
                                  'rtk_nasha', 'ttk_nasha', 'phone', 'stoplist_c', 'ptv_n'], as_index=False, dropna=False).\
                                                                                                agg({'phone_work': 'count'}). \
                                                                                                sort_values('phone_work').\
                                                                                                rename(columns={'phone_work': 'contacts'}).\
                                                                                                reset_index()
        
        # Определим сколько дней телефон в отложке

        print('____rest_days defination')
        df_group['rest_days'] = pd.Timestamp.now().normalize() - df_group['last_call']
        df_group['rest_days'] = df_group['rest_days'].astype('str').apply(lambda x: x.strip(' days')).astype('int64')

        # Определим поле с приоритетами

        df_group['priority2'] = df_group['priority2'].replace(dict_project)
        df_group['priority1'] = df_group['priority1'].replace(dict_project)

        # Определим поле источник, заменим устаревшие значения на знаячение 100500

        df_group['base_source_c'] = df_group['base_source_c'].replace({'' : 'Пусто'})
        df_group['base_source_c'] = df_group['base_source_c'].fillna('100500').apply(lambda x: x if any(a in x for a in source_list) else '^100500^')

        print('___defination base_source_c')

        # Приведем числовые поля к нужжным типам данных

        col_list_int = [ 'ntv_ptv', 
                'bln_nasha', 'mts_nasha', 'nbn_nasha', 'dom_nasha', 'rtk_nasha', 'ttk_nasha',
                'ptv_n']

        df_group[col_list_int] = df_group[col_list_int].astype('int8')

        df_group['contacts'] = df_group['contacts'].astype('int64')
        df_group['rest_days'] = df_group['rest_days'].astype('int64')

        del df_group['index']

        # Оставим только нужные шаги, остальные заменим на значение 100500
    
        df_group['step_c'] = df_group['step_c'].fillna('100500').apply(lambda x: x if x in step_list else '100500')

        # В поле со стоплистами почистим пустые значения

        df_group['stoplist_c'] = df_group['stoplist_c'].fillna('').replace('', 'Пусто').replace(' ', 'Пусто').replace('None', 'Пусто')
        

        #  Загружаем данные на CH
        download_to_click(table_name_ch, sql_download, df_group)
        start += end

        print('__download df_', start)

        sleep(timeout)

    
    
    

  


