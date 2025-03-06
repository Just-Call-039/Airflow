import os
import pandas as pd

def union_csv(path_to_folder, start, end, final_name):

    from datetime import datetime
    from datetime import timedelta
    # from datetime import date

    start = datetime.strptime(start, '%Y-%m-%d').date()
    end = datetime.strptime(end, '%Y-%m-%d').date()
    print(start)


    daterange = [(start + timedelta(days=x)) for x in range(0, (end - start).days)]


    
    file_list = ['Ждуны за {}.csv'.format(date_i) for date_i in daterange]

    print(file_list)
    
    i=0
    
    for filename in os.listdir(f'{path_to_folder}'):
        if filename in file_list:
            if i == 0:
                df = pd.read_csv(f'{path_to_folder}/{filename}')
                i += 1
            else:
                df = pd.concat([
                    df,
                    pd.read_csv(f'{path_to_folder}/{filename}')],
                    ignore_index = True,
                axis=0)

    col_list = ['ochered', 'last_step', 'caller_id']
    
    for col in col_list:
        df[col] = df[col].astype('str').apply(lambda x: x.replace('.0', ''))
    print(df[df['caller_id'] == '89124288081']['ochered'])

    # Сохраняем полученный датасет
    df.to_csv(f'{path_to_folder}/{final_name}', index = False)

    print('___save_fie__', final_name)
        

