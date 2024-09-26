import os
# Скрипт для очистки файлов в папке с сервера 

def clear_folder(folder):

    folder_path = f'{folder}'  # Путь к папке, которую нужно очистить
    os.listdir(folder_path)

    for file_name in os.listdir(folder_path):
        if file_name != 'Подробные маршруты.csv' and file_name != 'Маршруты уникальные.csv':

            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
            
                os.remove(file_path)
                print('__delete ', file_name)
            elif os.path.isdir(file_path):
            
                for file in os.listdir(file_path):             
                    path_to_file = os.path.join(file_path, file)
                    os.remove(path_to_file)
                    print('__delete ', file_name)
