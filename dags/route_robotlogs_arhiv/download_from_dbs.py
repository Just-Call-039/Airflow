# Загрузка файлов с dbs 

def transfer_file_from_dbs(file_path_on_share, local_file_path):

        
    from smb.SMBConnection import SMBConnection, OperationFailure

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
    
    

    