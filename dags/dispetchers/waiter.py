import pandas as pd
from indicators_to_regions import download_googlesheet

def set_project(waiter_path, result_path):

    type_dict = {'caller_id' : 'str', 'queue_num_curr' : 'str'}

    waiter_df = pd.read_csv(waiter_path, dtype=type_dict)

    project_df = download_googlesheet.download_gs('Группировка очередей', 'Лист1')

    project_df.rename(columns = {'Очередь' : 'queue_num_curr', 
                                 'Проект (набирающая очередь)' : 'project'}, inplace=True)
    project_df['queue_num_curr'] = project_df['queue_num_curr'].fillna('').astype('str')
    project_df['queue_num_curr'] = project_df['queue_num_curr'].fillna('').astype('str')

    merge_df = waiter_df.merge(project_df[['queue_num_curr', 'project']], how = 'left', on = 'queue_num_curr').fillna('')



    merge_df['queue_zalivki'] = merge_df['project'].apply(lambda x: 
                                                          '9297' if 'RTK' in x 
                                                                 else 
                                                                    '9295' if 'MTS' in x 
                                                                           else 
                                                                                '9052' if 'Tele2' in x
                                                                                       else 
                                                                                            '9299' if 'DOMRU' in x
                                                                                                   else 
                                                                                                        '9293' if 'TTK' in x
                                                                                                            else 
                                                                                                                '9298' if 'NBN' in x
                                                                                                                       else 
                                                                                                                           '9296' if 'BEELINE' in x
                                                                                                                                else 
                                                                                                                                    '9072' if 'GULFSTREAM' in x
                                                                                                                                            else '')

    merge_df.to_excel(result_path, index = False)

def set_project_money(waiter_path, result_path):

    type_dict = {'caller_id' : 'str', 'queue_num_curr' : 'str'}

    waiter_df = pd.read_csv(waiter_path, dtype=type_dict)

    # project_df = download_googlesheet.download_gs('Группировка очередей', 'Лист1')

    # project_df.rename(columns = {'Очередь' : 'queue_num_curr', 
    #                              'Проект (набирающая очередь)' : 'project'}, inplace=True)
    # project_df['queue_num_curr'] = project_df['queue_num_curr'].fillna('').astype('str')
    # project_df['queue_num_curr'] = project_df['queue_num_curr'].fillna('').astype('str')

    # merge_df = waiter_df.merge(project_df[['queue_num_curr', 'project']], how = 'left', on = 'queue_num_curr').fillna('')
    # print(merge_df.columns)

    waiter_df['queue_zalivki'] = waiter_df['project'].apply(lambda x: 
                                                          '9297' if 'RTK' in x 
                                                                 else 
                                                                    '9295' if 'MTS' in x 
                                                                           else 
                                                                                '9052' if 'Tele2' in x
                                                                                       else 
                                                                                            '9299' if 'DOMRU' in x
                                                                                                   else 
                                                                                                        '9293' if 'TTK' in x
                                                                                                            else 
                                                                                                                '9298' if 'NBN' in x
                                                                                                                       else 
                                                                                                                           '9296' if 'BEELINE' in x
                                                                                                                                else 
                                                                                                                                    '9072' if 'GULFSTREAM' in x
                                                                                                                                            else '')

    waiter_df.to_excel(result_path, index = False)
