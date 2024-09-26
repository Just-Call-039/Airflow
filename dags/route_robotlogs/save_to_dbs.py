# Загрузка файлов с dbs

from commons import transfer_file_to_dbs
import os

def save_to_dbs(path_from, path_to):

    for filename in os.listdir(f'{path_from}'):
        transfer_file_to_dbs.transfer_file_to_dbs(path_from, path_to, filename, 'DBS')
        print('___save_fie__', filename)
        
