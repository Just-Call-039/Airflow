import pandas as pd
from indicators_to_regions import download_googlesheet

def transfer_edit(call_path, step_path, date_i, result_path):

    year = date_i.year
    month = date_i.month
    day = date_i.day

    step_path = f'{step_path}steps_{year:02}_{month:02}_{day:02}.csv'

    type_dict = {'phone' : 'str', 'dialog' : 'str', 'last_step' : 'str', 'step' : 'str', 'ochered' : 'str'}

    call_df = pd.read_csv(call_path, dtype=type_dict)
    print(call_df['dialog'].unique())

    step_df = pd.read_csv(step_path, dtype= type_dict)
    step_df['type_steps'] = step_df['type_steps'].fillna(0).astype(int)
    

    step_df.rename(columns={'step' : 'last_step',
                           'ochered' : 'dialog',
                           'type_steps' : 'type_step'}, inplace = True)
    
    print(call_df.columns)
    print('call', call_df.shape[0])

    call_df = call_df.merge(step_df[['last_step', 'dialog', 'type_step']], how = 'left', 
                                on = ['last_step', 'dialog'])
    print(step_df['dialog'].unique())
    print(call_df['dialog'].unique())
    
    print('type_step', call_df['type_step'].unique())
    call_df = call_df.fillna('')
    call_df = call_df[call_df['type_step'] != '']
    call_df['type_step'] = call_df['type_step'].astype(int)
    # call_df['type_step'] = call_df['type_step'].fillna(0).astype(int)
    print('type_step', call_df['type_step'].unique())
    # call_df = call_df[call_df['type_step'] == 1]
    print(call_df['dialog'].unique())
    print('call_df ', call_df['dialog'].shape[0])
    # call_df = call_df.fillna('')
    

    project_df = download_googlesheet.download_gs('Группировка очередей', 'Лист1')

    project_df.rename(columns = {'Очередь' : 'dialog', 
                                 'Проект (набирающая очередь)' : 'project'}, inplace=True)
    
    project_df['dialog'] = project_df['dialog'].fillna('').astype('str')
    project_df['dialog'] = project_df['dialog'].fillna('').astype('str')

    merge_df = call_df.merge(project_df[['dialog', 'project']], how = 'left', on = 'dialog').fillna('')
    print(merge_df['dialog'].unique())
    print('merge_df size', merge_df['dialog'].shape[0])



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
    merge_df = merge_df[['phone', 'call_date', 'dialog', 'contacts_status_c', 'project', 'queue_zalivki']]
    print('merge_df size', merge_df['dialog'].unique())
    merge_df.to_excel(result_path, index = False)