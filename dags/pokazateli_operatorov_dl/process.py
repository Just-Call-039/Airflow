import pandas as pd 
from commons_liza import defs, google_sheet
from pokazateli_operatorov_dl import functions



def call_merge(call_csv, request_csv, user_csv, city_csv, type_dict):

   # Загружаем датасет со звонками 

   df = pd.read_csv(call_csv, dtype = type_dict)
   print('loaded calls since ', df['call_date'].min())
   print('call size', df.shape[0])

   df['call_date'] = pd.to_datetime(df['call_date'])

   # Объединяем датафрейм с городами
   print('start merge df & city')
   city = pd.read_csv(city_csv,  dtype = type_dict)
   city['Город'] = city['Город'].apply(defs.del_staple)
   df = df.merge(city[['city_c', 'Город']], left_on = 'city', right_on = 'city_c', how = 'left').fillna('')
   print('size df after merdge city: ', df.shape[0])

   # Объединяем датафрейм с пользователями
   print('merge df & users')
   users = pd.read_csv(user_csv, dtype = type_dict).fillna('')
   df = df.merge(users, left_on = 'user_call', right_on = 'id', how = 'left').fillna('')
   print('date since after merge ', df['call_date'].min())
   print('size df ', df.shape[0])

   # Загрузим датасеты с лидами и проектами
      
   lids = google_sheet.download_gs('Команды/Проекты', 'Лиды')
   jc = google_sheet.download_gs('Команды/Проекты', 'JC')
   
   # merge с лидами
   print('merge df & lids')
   df =  df.merge(lids[['Проект','СВ CRM', 'МРФ']], left_on = 'supervisor', right_on = 'СВ CRM', how = 'left').fillna('')
   print('date since after merge ', df['call_date'].min())
   print('size df ', df.shape[0])

   # merge с проектами
   print('merge df & jc')
   df =  df.merge(jc[['Проект','CRM СВ']], left_on = 'supervisor', right_on = 'CRM СВ', how = 'left').fillna('')
   print('date since after merge ', df['call_date'].min())
   print('size df ', df.shape[0])

   # Заполняем поле проекты

   df.apply(lambda row: functions.update_project(row), axis=1)

#  Оставим только нужные поля в датафрейму, дубли удалим и переименуем столбцы

   df = df[['id_x',
            'call_date',
            'name',
            'contactid',
            'queue',
            'user_call',
            'super',
            'Город',
            'Область',
            'call_sec',
            'short_calls',
            'dialog',
            'completed_c',
            'fio',
            'supervisor',
            'Проект_x',
            'МРФ',
            'call_count',
            'phone']].rename(columns={'id_x': 'id',
                                       'Город': 'city',
                                       'Область': 'town',
                                       'Проект_x': 'project',
                                       'МРФ' : 'region'})

# Джойним датафрейм с пользователями доп, чтобы подгурзить инфо про penalty, sip, 
# и первый рабочий день

 