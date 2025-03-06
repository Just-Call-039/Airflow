from server_compare import defs
import pandas as pd


def proccess_server_df(file_path, robotlog_filename, debug_parse_filename, step_file, queue_file, user_filename, team_file, quality_file, city_file):

    # Загружаем robotlog
    robotlog_df = pd.read_csv(f'{file_path}{robotlog_filename}')
    print('download robotlog_df - size ', robotlog_df.shape[0])

    # Загрузим шаги, чтобы определить был ли перевод на звонке

    step_df = pd.read_csv(step_file)

    # Переименуем столбцы и приведем к нужным типам для дальнейшего объединения

    step_df = step_df.rename(columns={'ochered' : 'dialog', 'step' : 'last_step'})


    step_df['dialog'] = step_df['dialog'].fillna('0').astype('str').apply(lambda x: x.replace('.0', ''))
    step_df['last_step'] = step_df['last_step'].fillna('0').astype('str').apply(lambda x: x.replace('.0', ''))
    robotlog_df['dialog'] = robotlog_df['dialog'].fillna('0').astype('str').apply(lambda x: x.replace('.0', ''))
    robotlog_df['last_step'] = robotlog_df['last_step'].fillna('0').astype('str').apply(lambda x: x.replace('.0', ''))
    robotlog_df['date'] = pd.to_datetime(robotlog_df['call_date'], format='%Y-%m-%d').astype('str')

    # Объединим шаги и роботлог, чтобы определить был ли перевод на звонке
    log_step_df = robotlog_df.merge(step_df[['dialog', 'last_step', 'type_steps']], how = 'left', on = ['dialog', 'last_step'] )
    print('size df after merge with steps: ', log_step_df.shape[0])
    print('unique type_steps: ', log_step_df['type_steps'].unique())

    # Уберем в столбцах лишние ".0"
    column_list_int = ['real_billsec', 'trunk_id', 'type_steps']

    for col in column_list_int:
        
        log_step_df[col] = log_step_df[col].fillna('0').astype('int64')

    log_step_df = log_step_df.rename(columns={'type_steps' : 'perevod'})

    # Определим был ли перевод успешным  

    log_step_df['perevod_done'] = log_step_df.apply(lambda row: defs.define_perevod_done(row['perevod'], row['client_status'], row['assigned_user_id']), axis=1)
   
    print('unique perevode_done ', log_step_df['perevod_done'].unique())

    # Определим была ли заявка на звонке
    log_step_df['request'] = log_step_df.apply(lambda row: defs.define_request(row['client_status'], row['perevod_done']), axis=1)
    print('unique_request ', log_step_df['request'].unique())

    # Соединим датафрейм с пользователями,командами и очередями, чтобы определить проект

    #### Пользователи

    df_user =  pd.read_csv(f'{file_path}{user_filename}')

    # Переименуем столбцы и приведем к нужным типам для дальнейшего объединения
    df_user = df_user.rename(columns={'id' : 'assigned_user_id'})

    # Заменим в датасете пользователи значение 1, чтобы не получить некорректные данные при объединение (в роботлог 1 - отсутствие оператора на звонке)

    df_user.loc[df_user['assigned_user_id'] == '1', 'assigned_user_id'] = 'Пусто'

    # Объединяем
    log_user_df = log_step_df.merge(df_user[['assigned_user_id', 'supervisor']], how = 'left', on = 'assigned_user_id')
    print('size after merge with user', log_user_df.shape[0])

    # Заполним пустые значения в столбце супервайзер

    log_user_df.supervisor = log_user_df.supervisor.fillna('Пусто')

    #### Команды

    team_df = pd.read_csv(team_file)

    # Удалим дубликаты, чтобы избежать дублирование во время мерджа

    team_df = team_df.drop_duplicates('supervisor')

    # Объеденим датафремы

    log_team_df = log_user_df.merge(team_df[['supervisor', 'project']], how = 'left', on = 'supervisor' )

    log_team_df.project = log_team_df.project.fillna('Пусто')
    print('size after merge with team', log_team_df.shape[0])
    print('unique project team ', log_team_df.project.unique())

    #### Очереди

    df_queue = pd.read_csv(queue_file)
    
    # Переименуем столбцы и приведем к нужным типам для дальнейшего объединения

    df_queue = df_queue.rename(columns = {'Очередь' : 'dialog', 'Проект (набирающая очередь)' : 'project'})
    df_queue['dialog'] = df_queue['dialog'].astype('str')

     # Объеденим датафремы

    log_queue_df = log_team_df.merge(df_queue[['dialog', 'project']], how = 'left', on = ['dialog'])
    print('size after merge with queue', log_queue_df.shape[0])
    
    log_queue_df.project_y = log_queue_df.project_y.astype(str).fillna('Пусто')
    log_queue_df.project_x = log_queue_df.project_x.astype(str).fillna('Пусто')
    log_queue_df['assigned_user_id'] = log_queue_df['assigned_user_id'].astype(str)
    
    print('unique project queue  ', log_queue_df.project_y.unique())

    # Определим значение проекта для каждого звонка (по тому, к какому проекту относится команда оператора, если нет оператора - то по очереди)

    log_queue_df['project'] = log_queue_df.apply(lambda row: defs.define_project(row['project_y'], row['project_x'], row['assigned_user_id']), axis = 1)
    print('unique project finally ', log_queue_df.project.unique())

    # Опреедлим качество базы на звонке

    log_queue_df['region_c'] = log_queue_df['region_c'].fillna('Пусто').astype('str')
    log_queue_df['ptv_c'] = log_queue_df['ptv_c'].fillna('Пусто').astype('str')

    log_queue_df['quality'] = log_queue_df.apply(lambda row: defs.define_quality(row['ptv_c'], row['region_c']), axis = 1) 

    # Обюъеденим датафрейм с датафреймом качество, чтобы достать значения качества базы

    quality_df = pd.read_csv(quality_file)

    # Заполним значение None пустыми строками в столбце Качество города

    quality_df['Качество города'] = quality_df['Качество города'].fillna('Пусто')

    # Убираем лишние скобки в столбце Качество города и переименуем столбец для аккуратного мерджа

    quality_df['Качество города'] = quality_df['Качество города'].apply(defs.delete_staple)    
    quality_df = quality_df.rename(columns = {'id' : 'quality'})

    # Объденим датафреймы

    log_quality_df = log_queue_df.merge(quality_df[['quality', 'Качество города']], how = 'left', on = 'quality')
    log_quality_df['quality'] = log_quality_df['Качество города'].fillna('Пусто') 

    print('size df after merge with quality ', log_quality_df.shape[0])

    # Загружаем датафрейм debug_parse

    parse_df = pd.read_csv(f'{file_path}{debug_parse_filename}', dtype = {'id' : 'str', 'server' : 'str'})

    # Переименовываем колонки

    parse_df = parse_df.rename(columns = {'sec' : 'parse_sec'})

    # Создадим столбец с уникальным id, включающим номер сервера для дальнейцшего джона с роботлогом
    print('размер датафрейма парсе_дф', parse_df.shape[0])
    print('колонки датафрейма парсе_дф', parse_df.columns)

    parse_df['uniqueid'] = parse_df.apply(lambda row: defs.set_uniqueid(row['uniqueid'], row['server_number']), axis=1)

    # Создаем отдельные датафреймы для каждого показателя, чтобы посредством джойна получить столбцы

    found_df = parse_df[parse_df['event_type'] == 'Found']
    search_df = parse_df[parse_df['event_type'] == 'search_text']
    sqltook_df = parse_df[parse_df['event_type'] == 'sql_took']

    # Джойним found
    df_log_parse_1 = log_quality_df.merge(found_df[['uniqueid', 'parse_sec']], how = 'left', on = 'uniqueid')
    df_log_parse_1 = df_log_parse_1.rename(columns={'parse_sec' : 'found'})
    print(df_log_parse_1.shape[0])
   

    # Джойним search
    df_log_parse_2 = df_log_parse_1.merge(search_df[['uniqueid', 'parse_sec']], how = 'left', on = 'uniqueid')
    df_log_parse_2 = df_log_parse_2.rename(columns={'parse_sec' : 'search_sec'})
    print(df_log_parse_2.shape[0])
    print(df_log_parse_2.search_sec.unique())

    # Джойним sqltook
    df_log_parse_3 = df_log_parse_2.merge(sqltook_df[['uniqueid', 'parse_sec']], how = 'left', on = 'uniqueid')
    df_log_parse_3 = df_log_parse_3.rename(columns={'parse_sec' : 'sqltook_sec'})
    print(df_log_parse_3.shape[0])
    print(df_log_parse_3.sqltook_sec.unique())

 
    result_df = df_log_parse_3[['date', 'last_step', 'dialog', 'server_number', 'autootvetchik', 'client_status', 'directory',  'phone', 'found', 'search_sec', 'sqltook_sec',
                        'marker', 'real_billsec', 'trunk_id', 'network_provider_c', 'city_c', 'town', 'perevod', 'perevod_done', 'request', 'project', 'quality']]
   


    # # Сгруппируем данные для уменьшения количества строк

    # result_df = log_queue_df[['date', 'last_step', 'dialog', 'server_number', 'autootvetchik', 'client_status', 'directory', 'phone', 'marker', 'real_billsec', 
    #                           'trunk_id', 'network_provider_c', 'city_c', 'town', 'perevod', 'perevod_done', 'request', 'project', 'quality']].\
    #                         groupby(['date', 'last_step', 'dialog', 'server_number', 'autootvetchik', 'client_status', 'directory',
    #                         'marker', 'real_billsec', 'trunk_id', 'network_provider_c', 'city_c', 'town', 'project', 'quality'], as_index = False).\
    #                         agg({'phone' : 'count', 'request' : 'sum', 'perevod' : 'sum', 'perevod_done' : 'sum'})
    
    print('size df after groupby ', result_df.shape[0])    

    # Переименуем столбцы для удобства

    result_df = result_df.rename(columns = {'town' : 'town_c'})

    # Мерджим с городами, чтобы достать названия городов и областей

    city_df = pd.read_csv(city_file)
    city_df['city_c'] = city_df['city_c'].fillna('0').apply(defs.fillnan_my).astype('str')
    city_df['town_c'] = city_df['town_c'].fillna('0').apply(defs.fillnan_my).astype('str')

    # Заполняем пустые значения и пробелы в полях с кодом города и областей нулем

    result_df['town_c'] = result_df['town_c'].fillna('0').apply(defs.fillnan_my).astype('str')
    result_df['city_c'] = result_df['city_c'].fillna('0').apply(defs.fillnan_my).astype('str')

    # Мерджим Датафрейм с датасетом city, достаем города
    
    result_df = result_df.merge(city_df[['city_c', 'Город']], how='left', on = 'city_c')

    # Мерджим датафрейм с датасетом city, достаем области
    town_df = city_df.drop_duplicates('town_c')
    result_df = result_df.merge(town_df[['town_c', 'Область']], how='left', on = 'town_c')
    print('size after merge with city and town', result_df.shape[0])

    # Удаляем столбцы с кодами обдластей и городов
    del result_df['town_c']
    del result_df['city_c']

    # Переименовываем поля со значениями область и города

    result_df = result_df.rename(columns = {'Область' : 'town', 'Город' : 'city'})

    result_df = result_df[['date', 'last_step', 'dialog', 'server_number', 'autootvetchik', 'client_status', 'directory',  'phone', 'found', \
                                'search_sec', 'sqltook_sec',
                        'marker', 'real_billsec', 'trunk_id', 'network_provider_c', 'city', 'town', 'perevod', 'perevod_done', 'request', 'project', 'quality']]

    result_df.to_csv(f'{file_path}{robotlog_filename}', index = False)    
















