# Выгрузка данных с сервера с помощью sql запросов

import pymysql
import pandas as pd
import datetime

words_dict = {

    'не понятно' : ['непонял', 'непонятно', 'что говорите', 'непонимаю'],
    'плохо слышно' :['неслышно', 'плохо слышно', 'плохо слышу', 'алло девушка', 'неслышу'],
    'область/город/район': ['область', 'район', 'город', 'башкортостан'],
    'негород' : ['негород', 'несело', 'недеревня', 'яневгороде', 'нев городе'],
    'называет оператора' : ['ростелеком', 'теле два', 'мтс', 'билайн', 'мегафон'],
    'приветствие' : ['здравс', 'привет', 'ну говорите',  'да говорите', 'слушаю'],
    'согласие' : [' да ', 'угу', 'понятно', 'ясно '],
    'нет возможности подключения' : ['уже смотрели', 'наш дом '],
    'уже подключен': ['уже подклю', 'я и так '],
    'автоответчик' : ['здравствуйте с вами говорит', 'первый сезон нашего разговора', 'готов говорить с вами',\
                      'здравствуйте вы позвонили', 'невполне понял', 'окей записал еще что то передать',\
                      'незнаю нужно ли мне что то такое мне и так']}

def creat_grupps(answer):
    

        grupp_word = ''
        for name_gr, words_list in words_dict.items():
            i=0
            for word in words_list:
                if i < 1:
                    if word in str(answer):

                        grupp_word = name_gr
                        i += 1
        return(grupp_word)      

def download_per_day(host_dict, path_to_folder, day_count):

    for i in range(3, day_count):

        # Определим дату, за которую будем выгружать данные

        date_f = datetime.date.today() - pd.Timedelta(days=i)
        print(date_f)

        # Сгенерируем название для файлов на основе этой даты
    
        i_date = '{}_{}_{}.csv'.format(date_f.year, '{0:0>2}'.format(date_f.month), '{0:0>2}'.format(date_f.day))
    
        name_calls = 'calls_{f}'.format(f=i_date)

        j = 0   
        for host_name, host in host_dict.items():
        
            Con = pymysql.connect(user='robot_read_only',
                                passwd='du9Itg5bnzTb',
                                host=host,
                                db='robot')
    
            table_list = pd.read_sql_query("SHOW TABLES", Con)
                
            i = 0
            for table in table_list['Tables_in_robot'].values:
        
                if table.startswith('cel_just-call-9') and len(table) < 25:
                    query = "SELECT * FROM `{}`\
                            WHERE type = 'repeat' \
                            AND date(date) = {}".format(table, date_f)
                    if i == 0:
                        df_1 = pd.read_sql_query(query, Con)
                        df_1['server'] = host_name
                        df_1['queue'] = table[14:18]
                        result = df_1
                    else:
                        new_df = pd.read_sql_query(query, Con)
                        new_df['server'] = host_name
                        new_df['queue'] = table[14:18]
                        result = pd.concat([new_df, df], ignore_index= True)
                
                    i+=1
                    df = result
            if j == 0:
                calls = df 
            else:
                call_full = pd.concat([calls, df], ignore_index= True)
                calls = call_full
            j+=1
        print('____calls download', calls.shape[0])
       
        calls['grupp_word'] = calls['full_normalized'].apply(creat_grupps)

        calls.to_csv(f'{path_to_folder}/{name_calls}', index = False) 


def download_from_db(host_dict, path_to_folder, file_name):

    j = 0   
    for host_name, host in host_dict.items():
        
        Con = pymysql.connect(user='robot_read_only',
                                passwd='du9Itg5bnzTb',
                                host=host,
                                db='robot')
    
        table_list = pd.read_sql_query("SHOW TABLES", Con)
                
        i = 0
        for table in table_list['Tables_in_robot'].values:
        
            if table.startswith('cel_just-call-9') and len(table) < 25:
                query = "SELECT * FROM `{}` WHERE type = 'repeat'".format(table)
                if i == 0:
                    df_1 = pd.read_sql_query(query, Con)
                    df_1['server'] = host_name
                    df_1['queue'] = table[14:18]
                    result = df_1
                else:
                    new_df = pd.read_sql_query(query, Con)
                    new_df['server'] = host_name
                    new_df['queue'] = table[14:18]
                    result = pd.concat([new_df, df], ignore_index= True)
                
                i+=1
                df = result
        if j == 0:
            calls = df 
        else:
            call_full = pd.concat([calls, df], ignore_index= True)
            calls = call_full
        j+=1
    print('____calls download', calls.shape[0])
    calls.to_csv(f'{path_to_folder}/{file_name}', index=False)    

def create_words_df(path_to_folder, file_name):

    # words_df = pd.DataFrame([

    # ['не понятно', ['непонял', 'непонятно', 'что говорите', непонимаю'],
    # ['плохо слышно', 'неслышно, плохо слышно, плохо слышу, алло девушка, неслышу'],
    # ['область/город/район', 'область, район, город, башкортостан'],
    # ['негород', 'негород, несело, недеревня, яневгороде, нев городе'],
    # ['называет оператора', 'ростелеком, теле два, мтс, билайн, мегафон'],
    # ['приветствие', 'здравс, привет, ну говорите,  да говорите, слушаю'],
    # ['согласие', 'да, угу, понятно, ясно '],
    # ['нет возможности подключения', 'уже смотрели, наш дом '],
    # ['уже подключен', 'уже подклю, я и так '],
    # ['автоответчик', 'здравствуйте с вами говорит, первый сезон нашего разговора, готов говорить с вами,\
    #                   здравствуйте вы позвонили, невполне понял, окей записал еще что то передать,\
    #                   незнаю нужно ли мне что то такое мне и так']]

    words_df = pd.DataFrame([

    ['не понятно', ['непонял', 'непонятно', 'что говорите', 'непонимаю']],
    ['плохо слышно', ['неслышно', 'плохо слышно', 'плохо слышу', 'алло девушка', 'неслышу']],
    ['область/город/район', ['область', 'район', 'город', 'башкортостан']],
    ['негород', ['негород', 'несело', 'недеревня', 'яневгороде', 'нев городе']],
    ['называет оператора', ['ростелеком', 'теле два', 'мтс', 'билайн', 'мегафон']],
    ['приветствие', ['здравс', 'привет', 'ну говорите',  'да говорите', 'слушаю']],
    ['согласие', ['да', 'угу', 'понятно', 'ясно ']],
    ['нет возможности подключения', ['уже смотрели', 'наш дом ']],
    ['уже подключен', ['уже подклю', 'я и так ']],
    ['автоответчик', ['здравствуйте с вами говорит', 'первый сезон нашего разговора', 'готов говорить с вами',\
                      'здравствуйте вы позвонили', 'невполне понял', 'окей записал еще что то передать',\
                      'незнаю нужно ли мне что то такое мне и так']]]
    )

    words_df.columns = ['группа', 'слова']
    words_df = words_df.explode('слова')

    words_df.to_csv(f'{path_to_folder}{file_name}', index = False)

def add_col_words(path_to_folder, file_name):

    calls = pd.read_csv(f'{path_to_folder}{file_name}')

    words_dict = {
    'не понятно' : ['непонял', 'непонятно', 'что говорите', 'непонимаю'],
    'плохо слышно' :['неслышно', 'плохо слышно', 'плохо слышу', 'алло девушка', 'неслышу'],
    'область/город/район': ['область', 'район', 'город', 'башкортостан'],
    'негород' : ['негород', 'несело', 'недеревня', 'яневгороде', 'нев городе'],
    'называет оператора' : ['ростелеком', 'теле два', 'мтс', 'билайн', 'мегафон'],
    'приветствие' : ['здравс', 'привет', 'ну говорите',  'да говорите', 'слушаю'],
    'согласие' : [' да ', 'угу', 'понятно', 'ясно '],
    'нет возможности подключения' : ['уже смотрели', 'наш дом '],
    'уже подключен': ['уже подклю', 'я и так '],
    'автоответчик' : ['здравствуйте с вами говорит', 'первый сезон нашего разговора', 'готов говорить с вами',\
                      'здравствуйте вы позвонили', 'невполне понял', 'окей записал еще что то передать',\
                      'незнаю нужно ли мне что то такое мне и так']}

    def creat_grupps(answer):
    

        grupp_word = ''
        for name_gr, words_list in words_dict.items():
            i=0
            for word in words_list:
                if i < 1:
                    if word in str(answer):

                        grupp_word = name_gr
                        i += 1
        return(grupp_word)      

    calls['grupp_word'] = calls['full_normalized'].apply(creat_grupps)

    calls.to_csv(f'{path_to_folder}{file_name}')

