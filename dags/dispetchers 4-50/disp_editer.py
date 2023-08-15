def disp_editor(path_to_files, req, path_result):
    import pandas as pd

    print('Request')
    request = pd.read_csv(f'{path_to_files}{req}').fillna('')
    request = request[['last_queue_c','proect','team_x','uid','fio_x','date_entered','status','konva','phone_work','supervisor']].rename(columns={'team_x':'team','fio_x':'fio'})

    req_file = 'meeting_phones.csv'
    print('Сохраняем файл')
    request.to_csv(rf'{path_result}/{req_file}', index=False, sep=',', encoding='utf-8')






    