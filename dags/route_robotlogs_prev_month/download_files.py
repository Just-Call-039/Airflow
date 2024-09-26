#  Загрузка датафрейма из нескольких файлов

def download_files(path_to_folder, file_name, date_f, final_path, final_name):
    
    import os
    import pandas as pd

    i=0

    
    start_name = '{file_name}_{y}_{m}_{d}.csv'.format(file_name = file_name, y = date_f.year, m = '{0:0>2}'.format(date_f.month), d = '01')
    end_name = '{file_name}_{y}_{m}_{d}.csv'.format(file_name = file_name, y = date_f.year,
                                                    m = '{0:0>2}'.format(date_f.month + 1),
                                                    d = '01')

    
    for filename in os.listdir(f'{path_to_folder}'):
        if (filename >= start_name) & (filename < end_name):
            if i == 0:
                df = pd.read_csv(f'{path_to_folder}/{filename}')
                i = i+1
            else:
                df = pd.concat([
                    df,
                    pd.read_csv(f'{path_to_folder}/{filename}')],
                    ignore_index = True,
                axis=0)

    # Сохраняем полученный датасет
    df.to_csv(f'{final_path}/{final_name}')

    print('___save_fie__', final_name)
        