def request_editer(path_to_files, request, path_result, file_result):
 import pandas as pd
 import glob
 
 csv_files = glob.glob('/root/airflow/dags/previous_month/Files/calls_with_request/*.csv')
 dataframes = []

 for file in csv_files:
    df = pd.read_csv(file)
    dataframes.append(df)

 Callreqfull = pd.concat(dataframes)

 Callreqfull.reset_index(drop=True, inplace=True)
 Callreqfull['rank'] = Callreqfull.groupby(['phone_number'])['call_date'].rank('dense', ascending=False)
 Callreqfull = Callreqfull[Callreqfull['rank'] == 1]

 request_now = pd.read_csv(f'{path_to_files}/{request}')

 request_now["my_phone_work"] = request_now['my_phone_work'].astype(object)
 request_now["my_phone_work"] = request_now['my_phone_work'].astype(str)

 Callreqfull["phone_number"] = Callreqfull['phone_number'].astype(object)
 Callreqfull["phone_number"] = Callreqfull['phone_number'].astype(str)

 request_now["user"] = request_now['user'].astype(object)
 request_now["user"] = request_now['user'].astype(str)

 Callreqfull["assigned_user_id"] = Callreqfull['assigned_user_id'].astype(object)
 Callreqfull["assigned_user_id"] = Callreqfull['assigned_user_id'].astype(str)

 print(f'Заявки {request_now.shape[0]}')

 Requests = request_now.merge(Callreqfull, how = 'left', left_on=['my_phone_work','user'], right_on=['phone_number','assigned_user_id'])

 print(f'Заявки после соединиения {Requests.shape[0]}')

 Requests = Requests[['project','request_date','user','super','status','last_queue_c','id_call','call_date','result_call_c','city','num','queue','assigned_user_id','rank']]
 
 Requests.to_csv(f'{path_result}/{file_result}',sep=',', index=False)

