def gar_into_click(zip_path):    
    import zipfile
    import xml.etree.ElementTree as ET
    import pandas as pd
    import re
    from clickhouse_driver import Client
    from datetime import datetime

    from commons.connect_db import connect_db
    from smb.SMBConnection import SMBConnection

    print('start')
    host, user, password = connect_db('DBS')
    conn = SMBConnection(username=user, password=password, my_name="Alexander Brezhnev", remote_name="samba", use_ntlm_v2=True)
   

    path = zip_path

    files = ['AS_HOUSES',
            'AS_ADDR_OBJ',
    'AS_ADM_HIERARCHY',
    'AS_MUN_HIERARCHY',
    'AS_CHANGE_HISTORY'
    ]
    not_files = ['AS_APARTMENT_TYPES',
    'AS_ADDR_OBJ_TYPES',
    'AS_ROOM_TYPES',
    'AS_OPERATION_TYPES',
    'AS_PARAM_TYPES',
    'AS_HOUSE_TYPES',
    'AS_ADDHOUSE_TYPES',
    'AS_OBJECT_LEVELS',
    'AS_NORMATIVE_DOCS_TYPES',
    'AS_NORMATIVE_DOCS_KINDS',
    'AS_REESTR_OBJECTS',
    'AS_STEADS',
    'AS_APARTMENTS',
    'AS_ROOMS',
    'AS_CARPLACES',
    'AS_ADDR_OBJ_DIVISION',
    'AS_ADDR_OBJ_PARAMS',
    'AS_STEADS_PARAMS',
    'AS_HOUSES_PARAMS',
    'AS_APARTMENTS_PARAMS',
    'AS_ROOMS_PARAMS',
    'AS_CARPLACES_PARAMS',
    'AS_NORMATIVE_DOCS'
    ]
    chk_no = '(?:{})'.format('|'.join(not_files))

    q = 0
    start = 0
    stop = 12

    print('заходим в условия')
    conn = SMBConnection(username=user, password=password, my_name="Alexander Brezhnev", remote_name="samba", use_ntlm_v2=True)
    if conn.connect(host, 445):
        print('OK')

        # results = conn.listPath('/', '')

        # for item in results:
        #     print(item.filename)

        archive = zipfile.ZipFile(zip_path, 'r')
        with zipfile.ZipFile(zip_path, mode='a') as zf:
            for i in files: # Смотрим нужные названия i
                print('Смотрим нужные названия i')
                
                chk_pat = '(?:{})'.format(i)
                print(i)


                for file in zf.namelist(): 
                    # Делаем проверку для каждого файла по названиям
                    w = 0

                    if bool(re.search(chk_pat, file, flags=re.I)):
                        if (bool(re.search(chk_no, file, flags=re.I))):
                            pass
                        else:
        #                         Открываем каждый файл, преобразуем в таблицу и заливаем в кликхаус
                            print(datetime.today().strftime("%m/%d/%Y, %H:%M:%S"))
                            print(file)
                            imgfile = archive.open(file)
                            tree = ET.parse(imgfile)
                            root = tree.getroot()

                            n = 0
                            w += 1
                            data = pd.DataFrame()
                            for child in root:
                                # Вывод атрибутов элемента
                                it = child.attrib
                                x = pd.DataFrame({k: [v] for k, v in it.items()})
                                data = data.append(x)

                                n += 1

                                if n%100000 == 0:
                                    print('connect ClickHouse')
                                    dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
                                    if dest:
                                        with open(dest) as file:
                                            for now in file:
                                                now = now.strip().split('=')
                                                first, second = now[0].strip(), now[1].strip()
                                                if first == 'host':
                                                    host = second
                                                elif first == 'user':
                                                    user = second
                                                elif first == 'password':
                                                    password = second

                                    print(f'insert_{n}')
                                    client = Client(host=host, port='9000', user=user, password=password,
                                                            database='gar', settings={'use_numpy': True})
                                    client.insert_dataframe(f'INSERT INTO gar.{i} VALUES', data)
