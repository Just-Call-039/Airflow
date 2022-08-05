# Функция для перемещения файла с сервера на сервер.
# Необходимо передать абсолютный путь на обоих серверах, название файла, наименование сервера, откуда перемещаем файл.


def transfer_file(from_path, to_path, file, db):
    import paramiko

    from time import sleep
    from scp import SCPClient
    from commons.connect_db import connect_db

    host, user, password = connect_db(db)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=user, password=password)

    sleep(10)

    scp = SCPClient(client.get_transport())

    # Откуда, куда.
    scp.get(f'{from_path}{file}', f'{to_path}{file}')
    scp.close()
    client.close()

    sleep(5)
