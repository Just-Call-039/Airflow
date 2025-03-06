from commons_liza import load_mysql, google_sheet, dbs, to_click
import pandas as pd
import os


# Функция, в которой загружаются с базы данных все файлы и сохраняются в csv. Манипуляции над ними минимальные. 
# Передаем в функцию:
#  пути к файлам sql (название переменных *_sql) для выгрузки данных
#  пути к файлам csv (названия переменных *_csv)
#  списки с данными для доступа в бд (cloud_*)
#  date_i дата, за которую выгрузаем данные
#  type_dict - словарь, в котором собранны все типы данных для столбцов во всех таблицах

def all_proccess(cloud_183, cloud_42, date_i, type_dict, contact_sql, contact_csv, gas_sql, gas_csv, robotlog_sql, robotlog_csv,
                     sms_sql, sms_csv, shedex_sql, shedex_csv, brigada_sql, brigada_csv):

    # Загружаем данные из таблиц контакты (из нее достанем номера телефонов, передеанные нам газификацией для отработки)

    load_mysql.get_data(contact_sql, cloud_183, date_i, contact_csv)

    contact = pd.read_csv(contact_csv, dtype = type_dict)

    # Создаем список с номерами от газификации

    phones = str(contact['phone'].fillna('000').to_list()).replace('[','').replace(']','')
    print(phones)

    # Передаем этот список в запрос sql для выгрузки данных из бд газификации

    # gas_request = open(gas_sql, "r", errors='ignore').read().format(phones= phones)
    gas_request = f'''select cleaned_phone as phone,  
                     contract, 
                     address, 
                     flat, 
                     brigade.name as brigade, 
                     territory_name, 
                     id_client_address, 
                     date plan_date, 
                     service
                     
                from gasification.client
                     left join gasification.client_address on id_client = client.id
                     left join gasification.address on address.id = id_address
                     left join gasification.brigade on id_brigade = brigade.id
                     left join gasification.client_service on client_service.id_client_address = client_address.id
                     left join gasification.service on service.id = id_service
            #    where client.cleaned_phone in ({phones})
                 '''

    # Загружаем данные из газификациии, бригады, заявки, адреса, контакты и тд

    # load_mysql.save_data_request(gas_request, cloud_42, gas_csv)
    gas = load_mysql.get_data_request(gas_request, cloud_42)
    gas = gas.merge(contact[['phone', 'call_date']], on = 'phone', how = 'left').fillna('')
    gas = gas[gas['call_date'] != '']
    del gas['call_date']
    gas.to_csv(gas_csv, index  = False)

    # Передаем в запрос переменные дату и список телефона, для того чтобы выгрузить данные по входящим звонкам из роботлога

    robotlog_request = open(robotlog_sql, "r", encoding='utf8', errors='ignore').read().format(phones = phones, date_i = date_i)

    # Загружаем данные по входящим на газ и сохраняем в csv файл
    robot_log = load_mysql.get_data_request(robotlog_request, cloud_183)
    contact = contact.rename(columns = {'call_date' : 'call_date_x'})
    robot_log = robot_log.merge(contact[['phone', 'call_date_x']], on = 'phone', how = 'left').fillna('')
    robot_log = robot_log[robot_log['call_date_x'] != '']
    del robot_log['call_date_x']
    robot_log.to_csv(robotlog_csv, index  = False)

    # load_mysql.save_data_request(robotlog_request, cloud_183, robotlog_csv)

    # Загружаем роботлог из файла для того, чтоюбы добавить проект в таблицу

    robotlog = pd.read_csv(robotlog_csv, dtype = type_dict)

    # Загружаем таблицу из google Группировка очередей, откуда достанем проекты

    table = 'Группировка очередей'
    sheet = 'Лист1'

    queue_df = google_sheet.download_gs(table, sheet)

    queue_df = queue_df.rename(columns = {'Проект (набирающая очередь)' : 'queue',
                                        'Группировка' : 'project'}).astype('str')
    
    # Джойним с роботлогом

    robotlog = robotlog.merge(queue_df, on = 'queue', how = 'left')[['phone', 'call_date', 'last_step', 'queue', 'inbound', 'project']]
    print('robotlog size', robotlog.shape[0])

    # Сохраняем входящие в csv файл
    robotlog.to_csv(robotlog_csv, index = False)

    # Загружаем и сохраняем в csv файл данные по shedex, отправленным смс и справочник по бригадам

    load_mysql.get_data(sms_sql, cloud_183, str(date_i), sms_csv)
    load_mysql.get_data(shedex_sql, cloud_183, str(date_i), shedex_csv)
    load_mysql.get_data(brigada_sql, cloud_42, str(date_i), brigada_csv)


# Функция для отправки полученных таблиц на dbs
# Передаем в функцию путь к папке проекта на airflow со всеми файлами и путь к папке проекта на dbs

def sent_to_dbs(folder_path, dbs_path):

    # Создаем список из файлов в переданной директории

    folder = os.listdir(folder_path)

    # Загруажаем файлы по порядку в циклеЖ

    for file in folder:

        print(file)
        n = ''

        # Определяем назание папки на dbs в которую надо загрузить файл. Берем ее название из названия файла:
        #  файл contact в папку contact, файл gas в папку gas и т.д.

        position = file.find('_')
        if position > 0:
            
            folder_name = f'\{n}' + file[:position] + f'\{n}'
            path_to = f'{dbs_path}{folder_name}{file}'

        else:

            position = file.find('.')
            folder_name = f'\{n}' + file[:position] + f'\{n}'
            path_to = f'{dbs_path}{folder_name}{file}'
            
        print('folder_name', folder_name)
        print('path_to', path_to)
        print('path_from', f'{folder_path}{file}')

        # Сохраняем файл в полученный путь
        
        dbs.save_file_to_dbs(f'{folder_path}{file}', path_to)



