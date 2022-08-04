# Функция для перемещения файла с сервера на сервер DBS.
# Необходимо передать абсолютный путь на обоих серверах, название файла, наименование сервера, куда перемещаем файл.


def transfer_file_to_dbs(from_path, to_path, file, db):
    from time import sleep
    from commons.connect_db import connect_db
    from smb.SMBConnection import SMBConnection

    host, user, password = connect_db(db)
    conn = SMBConnection(username=user, password=password, my_name="Alexander Brezhnev", remote_name="samba", use_ntlm_v2=True)

    sleep(5)

    if conn.connect(host, 445):
        with open(f'{from_path}{file}', 'rb') as my_file:
            conn.storeFile('dbs', f'{to_path}{file}', my_file)
    conn.close()

    sleep(5)
