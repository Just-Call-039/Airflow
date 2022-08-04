# Функция для удаления файла с сервера.
# Сервер MySQL не позволяет записывать файл из SQL запроса, если такой файл уже существует.
# Необходимо передать абсолютный путь на сервере, название файла, наименование сервера, откуда удаляем файл.


def del_file(from_path, file, db):
    import paramiko

    from time import sleep
    from commons.connect_db import connect_db

    host, user, password = connect_db(db)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=user, password=password)

    # Откуда.
    stdin, stdout, stderr = client.exec_command(f'rm -f {from_path}{file}')
    client.close()

    sleep(5)
