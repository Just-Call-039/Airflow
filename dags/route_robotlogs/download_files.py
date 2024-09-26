#  Загрузка датафрейма из нескольких файлов

def download_files(path_to_folder, final_path, final_name):
    
    import os
    import pandas as pd

    i=0
    
    for filename in os.listdir(f'{path_to_folder}'):
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
        