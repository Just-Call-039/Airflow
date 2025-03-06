

import os
from time import sleep
from commons.connect_db import connect_db
from smb.SMBConnection import SMBConnection
from smb.SMBConnection import SMBConnection, OperationFailure

def save_folder_to_dbs(path_from, path_to):
    


    for filename in os.listdir(f'{path_from}'):


        host, user, password = connect_db('DBS')
        conn = SMBConnection(username=user, password=password, my_name="Alexander Brezhnev", remote_name="samba", use_ntlm_v2=True)

        sleep(5)

        if conn.connect(host, 445):
            with open(path_from, 'rb') as my_file:
                conn.storeFile('dbs', path_to, my_file)
        conn.close()

        sleep(5)

        print('___save_fie__', filename)
        

def save_file_to_dbs(path_from, path_to):
    
    host, user, password = connect_db('DBS')
    conn = SMBConnection(username=user, password=password, my_name="Alexander Brezhnev", remote_name="samba", use_ntlm_v2=True)

    sleep(5)

    if conn.connect(host, 445):
        print('connection ok!')
        with open(path_from, 'rb') as my_file:
            print('file_to_save', my_file) 
            conn.storeFile('dbs', path_to, my_file)
            print('save done')
            
    conn.close()

    sleep(5)



# Загрузка файлов с dbs 

def transfer_file_from_dbs(file_path_on_share, local_file_path):

    server_ip = '192.168.1.157'
    server_name = 'servername'
    username = 'user_dbs01'
    password = 'tZSzfjLEkD95'
    share_name = 'dbs'
    
    try:
        conn = SMBConnection(username, password, 'my_machine_name', server_name, use_ntlm_v2=True)
        connected = conn.connect(server_ip, 445)

        if connected:
            try:
                with open(local_file_path, 'wb') as file_obj:
                    conn.retrieveFile(share_name, file_path_on_share, file_obj)

            except OperationFailure as e:
                print(f'Ошибка при попытке скачать файл: {e}')
        else:
            print("Не удалось подключиться к Samba серверу")

    finally:
        conn.close()