import pandas as pd
import datetime
import logging
import glob

logging.basicConfig(level=logging.INFO)



def set_minute(minute, second):
    
    if second >= 56:
        return minute + 1
    else:
        return minute
    
def set_hour(hour, minute):
    
    if minute == 60:
        return hour + 1
    else:
        return hour
    
def set_minute_60(minute):
    
    if minute == 60:
        return 0
    else:
        return minute
    
    
    
def set_active(active_list, df):
    
    for active in active_list:
        col = '_'.join(['active', active])
        df[col] = df['active'].apply(lambda x: 1 if x == active else 0)

def set_lastapp(lastapp_list, df):
    
    for lastapp in lastapp_list:
        col = '_'.join(['lastapp', lastapp])
        df[col] = df['lastapp_a'].apply(lambda x: 1 if x == lastapp else 0)
        
    if 'lastapp_Goto' not in lastapp_list:
        df['lastapp_Goto'] = 0

    
def set_cell_values(x, y):

    if x == '0':
        return y
    else:
        return x
    
def column_choose(col_list, df):
        
    for col in col_list:

        x_col = '_'.join([col, 'x'])
        y_col = '_'.join([col, 'y'])

        df[col] = df.apply(lambda row: set_cell_values(row[x_col], row[y_col]), axis = 1)

    return df

def set_request(status, perevod, userid):

    if (status == 'MeetingWait') & (perevod == 1) & (userid not in ['', ' ', '0', '1']):
        return 1
    else:
        return 0
    
def set_gaz_active(exit_point):

    if (exit_point == '1') | (exit_point == '999'):
        return 'hangup'
    else:

        return 'robot'

def set_null_active(exit_point):

    if exit_point in ['0', '5', '105']:
        return 'hangup'
    else:
        if exit_point in ['3', '4', '7', '100', '103', '104', '107']:
            return 'operator'
        elif exit_point in ['2', '6', '8', '9', '10', '11', '12', '13',
                            '102', '108', '109', '110', '111', '112', '113']:
            return 'robot'
        else:
            return 'other'
        
def truba_create_columns(truba_df):
    
    create_times_columns(truba_df, 'date_t')
    truba_df['date'] = truba_df['date_t'].astype('datetime64[ns]').dt.date
    
    truba_df['NN'] = truba_df.groupby(['phone', 'hour']).cumcount() + 1
    truba_df['uniqueid'] = truba_df['uniqueid'].apply(lambda x: x[:10])


def create_times_columns(df, date_col):
    
    df[date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d %H:%M:%S')
    
    df['hour'] = df[date_col].dt.hour    
    df['minute'] = df[date_col].dt.minute   
    df['second'] = df[date_col].dt.second


# Джойним с шагами

def set_perevod(step_path, robot_df, type_dict, date_start, numdays):

    date_list = [date_start - datetime.timedelta(days=x) for x in range(0, numdays)]

  
    step_df = pd.DataFrame()

    for date_i in date_list:

        year = date_i.year
        month = date_i.month
        day = date_i.day

        csv_name = f'steps_{year:02}_{month:02}_{day:02}.csv'

        df = pd.read_csv(f'{step_path}{csv_name}', dtype= type_dict)
        step_df = pd.concat([step_df, df])

    # step_df = pd.read_csv(step_path, dtype= type_dict)

    step_df.rename(columns={'step' : 'last_step',
                           'ochered' : 'queue_r',
                           'type_steps' : 'type_step',
                           'date': 'date_merge'}, inplace = True)

    step_df[['last_step', 'queue_r']] = step_df[['last_step', 'queue_r']].astype('str')

    robot_df = robot_df.merge(step_df, how = 'left', 
                                on = ['date_merge', 'last_step', 'queue_r'])
    
    robot_df['type_step'] = robot_df['type_step'].fillna(0).astype(int)
    robot_df = robot_df.fillna('')
    return robot_df






    


def astin_grouped(astin_df):
    
    astin_df = astin_df.fillna('0').groupby(['date_a', 'phone', 'lastapp_a', 'active', 'userfield'], as_index = False)\
                                        ['billsec_a'].agg({'billsec_a' : 'mean'})
    
    
    
    create_times_columns(astin_df, 'date_a')
    astin_df['date'] = astin_df['date_a'].astype('datetime64[ns]').dt.date
    astin_df['NN'] = astin_df.groupby(['phone', 'hour']).cumcount() + 1
        
    astin_df = astin_df.fillna('')
    
    active_list =  astin_df.active.unique()
    set_active(active_list, astin_df)
    
    print(astin_df.columns)
    print(astin_df['lastapp_a'].unique())
    lastapp_list = ['Dial', 'Transfer', 'Playback', 'BackGround', 'Hangup', 'WaitExten', 'Answer', 'Goto']
    set_lastapp(lastapp_list, astin_df)
    
    astin_df['userfield'] = astin_df['userfield'].apply(lambda x: x[:10])
    astin_df = astin_df[['date', 'phone', 'userfield', 'hour', 'minute', 'NN', 'billsec_a', 'active_robot', 'active_0', 'active_operator',
                                         'lastapp_Dial',
                                        'lastapp_Transfer', 'lastapp_Playback', 'lastapp_BackGround',
                                        'lastapp_Hangup', 'lastapp_WaitExten', 'lastapp_Answer',
                                        'lastapp_Goto']]
    
    logging.info(f'size astin grouped: {astin_df.shape[0]}')
    
    return astin_df
    










def merge_dfs(truba_df, astin_df): 
  
    union_df = truba_df.merge(astin_df,
                              how = 'left', 
                              left_on = ['date', 'phone', 'hour', 'minute', 'NN'], 
                              right_on = ['date',  'phone', 'hour', 'minute', 'NN']).fillna('0')
    count_astin = union_df[union_df['userfield'] != '0']
    logging.info(f'union_df size: {union_df.shape[0]}')
    logging.info(f'count_astin size: {count_astin.shape[0]}')
    
    
    union_df['count'] = 1
    union_df['daily_count'] = union_df.groupby('phone')['count'].transform('sum')
    union_df['billsec_t'] = union_df['billsec_t'].astype('int64')
    union_df['billsec_a'] = union_df['billsec_a'].astype('int64')
    union_df['spam'] = union_df['daily_count'].apply(lambda x: 1 if x > 3 else 0) 
    
    spam = union_df[union_df['spam'] == 1]
    notspam = union_df[union_df['spam'] == 0]
    logging.info(f'count of spam: {spam.shape[0]}')                                
    logging.info(f'count of live: {notspam.shape[0]}') 
    
    return union_df