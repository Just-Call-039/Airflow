
# Скрипт для очистки файлов в папке с сервера 
def clear_folder(folder):
    import pandas as pd
    import pymysql
    import datetime
    import os


    folder_path = f'{folder}'  # Путь к папке, которую нужно очистить
    files = os.listdir(folder_path)

    for file_name in files:
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)
        elif os.path.isdir(file_path):
            os.rmdir(file_path)