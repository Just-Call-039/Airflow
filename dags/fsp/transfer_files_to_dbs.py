# Функция для перемещения файла с сервера на сервер DBS.
# Необходимо передать абсолютный путь на обоих серверах, название файла, наименование сервера, куда перемещаем файл.


def transfer_files_to_dbs(from_path, to_path, db):
    import os
    import glob
    from time import sleep
    from commons.connect_db import connect_db
    from smb.SMBConnection import SMBConnection

    host, user, password = connect_db(db)
    conn = SMBConnection(username=user, password=password, 
                         my_name="Alexander Brezhnev", 
                         remote_name="samba", use_ntlm_v2=True)

    files = os.listdir(from_path)
    sleep(5)

    # if conn.connect(host, 445):
    #     for i in files:
    #         with open(f'{from_path}{i}', 'rb') as my_file:
    #             print(f'{from_path}{i}')
    #             conn.storeFile('dbs', f'{to_path}{i}', my_file)
    #             sleep(5)
    
    for i in files:
        conn = SMBConnection(username=user, password=password,
                              my_name="Alexander Brezhnev", 
                              remote_name="samba", use_ntlm_v2=True)
        if conn.connect(host, 445):
            with open(f'{from_path}{i}', 'rb') as my_file:
                print(f'{from_path}{i}')
                conn.storeFile('dbs', f'{to_path}{i}', my_file)
                sleep(5)

    
        conn.close()

    sleep(5)

def transfer_file_to_dbs(from_path, to_path, db, file1, file2):
    import os
    import glob
    from time import sleep
    from commons.connect_db import connect_db
    from smb.SMBConnection import SMBConnection

    host, user, password = connect_db(db)
    conn = SMBConnection(username=user, password=password, my_name="Alexander Brezhnev", remote_name="samba", use_ntlm_v2=True)

    files = os.listdir(from_path)
    sleep(5)

    if conn.connect(host, 445):
        with open(f'{from_path}{file1}', 'rb') as my_file:
            conn.storeFile('dbs', f'{to_path}{file1}', my_file)
            sleep(5)
        if file2 != '':
            with open(f'{from_path}{file2}', 'rb') as my_file:
                conn.storeFile('dbs', f'{to_path}{file2}', my_file)
                sleep(5)
    
    conn.close()

    sleep(5)

def remove_files_from_airflow(paths):
    import os
    import glob

    for folder in paths:
        files = glob.glob(folder+'*')
        for f in files:
            os.remove(f)