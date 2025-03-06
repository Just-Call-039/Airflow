import pandas as pd
from inbound_report import download_date_clickhouse
from commons_liza import to_click
from time import sleep


def save_data(result_path, type_dict, date_before):

    union_df = pd.read_csv(result_path, dtype = type_dict)
    print('Transfer count ', union_df[union_df['lastapp_Transfer'] == 1]['lastapp_Transfer'].sum())
    print('Dial count ', union_df[union_df['lastapp_Dial'] == 1]['lastapp_Dial'].sum())
    print('Playback count ', union_df[union_df['lastapp_Playback'] == 1]['lastapp_Playback'].sum())

    col_list_int = ['billsec_t', 'count',
                   'daily_count', 'spam', 'billsec_r', 'request_r', 'type_step', 'request_c']

    col_list_str = ['phone', 'lastapp_t', 'last_step', 'queue_i', 'active', 'date_c', 'date_r',
                   'exit_point', 'exit_name', 'project', 'queue_r', 'otkaz_c', 'queue_c']

    union_df[col_list_str] = union_df[col_list_str].astype(str).replace('.0', '', regex=False)
    union_df[col_list_int] = union_df[col_list_int].astype('int64')

    union_df.rename(columns = {'date_t' : 'call_date'}, inplace = True)

    union_df['call_date'] = pd.to_datetime(union_df['call_date'], format='%Y-%m-%d %H:%M:%S')
    print(union_df.shape[0])
    print(union_df.info())
    
    
    
    union_df = union_df[['call_date', 
                        'phone', 
                        'billsec_t', 
                        'lastapp_t',
                        'active',
                        'exit_point', 
                        'project', 
                        'count',
                        'daily_count', 
                        'spam', 
                        'date_r', 
                        'last_step', 
                        'queue_i', 
                        'billsec_r', 
                        'queue_r', 
                        'date_c', 
                        'otkaz_c', 
                        'queue_c', 
                        'active_robot', 
                        'active_0',                  
                        'active_operator',           
                        'lastapp_Dial',             
                        'lastapp_Transfer',           
                        'lastapp_Playback',        
                        'lastapp_BackGround',        
                        'lastapp_Hangup',         
                        'lastapp_WaitExten',           
                        'lastapp_Answer',              
                        'lastapp_Goto',
                        'request_r', 
                        'type_step', 
                        'request_c', 
                        'exit_name']]
    
    print(union_df.info())

    to_click.delete_data_per_period('inbound_report', 'call_date', date_before)

    sleep(600)

    download_date_clickhouse.save_data('inbound_report', union_df)












def save_request(result_path):

    request_df = pd.read_csv(result_path)

    col_list_str = ['userid', 'phone', 'statused', 'queue', 'regions']

    request_df[col_list_str] = request_df[col_list_str].astype(str).replace('.0', '', regex=False)

    

    request_df['dateentered'] = pd.to_datetime(request_df['dateentered'], format='%Y-%m-%d %H:%M:%S')
    print(request_df.shape[0])
    print(request_df.info())
    
    print(request_df.info())
    download_date_clickhouse.save_data('request', request_df)

    
