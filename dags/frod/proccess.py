import pandas as pd
import datetime
from frod import defs
import logging


logging.basicConfig(level=logging.INFO)

def merge_df(project_path, contact_csv, robotlog_csv, result_path, frod_csv, step_path, step_csv, city_path, quality_path):


   # Функция джойнит роботлог с контактами и сохраняет полученный датафрейм в csv для отчета фродилки
   #  path - путь к файлам проекта, 
   #  contact_csv - датафрейм из таблицы contacts_cstm, 
   #  robotlog_csv - датафрейм из таблицы роботлог, 
   #  path_result - путь к итоговому датафрейму, который потом на dbs отправляется, 
   #  frod_csv - название для итогового датафрейма, 
   #  path_step - путь к датафрейму с шагами, 
   #  step_csv - название файла с инофрмацией о шагах
    


   # Загружаем датафреймы

   contact_df = pd.read_csv(f'{project_path}{contact_csv}', sep = ';')
   logging.info(f'_____size contact df: {contact_df.shape[0]}')

   if contact_df.shape[0] > 0:

      robotlog_df = pd.read_csv(f'{project_path}{robotlog_csv}', sep = ';')
      logging.info(f'_____robotlog size: {robotlog_df.shape[0]}')
      

      # Джойним датаферймы

      frod_call = contact_df.merge(robotlog_df, how = 'left', on = ['id_c', 'phone'])
      frod_call['inbound_call'] = frod_call['inbound_call'].fillna(0)
      logging.info(f'_____size after merge: {frod_call.shape[0]}')

      # Определяем дата звонка бв роботлог была после фродилки или не связана с ней

      frod_call['last_call_c'] = frod_call['last_call_c'].astype('str')
      frod_call['call_date'] = frod_call['call_date'].astype('str')

      frod_call['diff_time'] = frod_call.apply(lambda row: defs.diff_time(row['last_call_c'], row['call_date']), axis = 1)
      frod_call['diff_time'] = frod_call['diff_time'].fillna(0)
      frod_call = frod_call.fillna('')

      # Фильтруем датафрейм от звонков, не связанных с фродилкой

      frod_call = frod_call[frod_call['diff_time'] == 1]

      # Возвращаем полям с датами тип данных датавремя. Создаем новое поле с датой
      frod_call['last_call_c'] = pd.to_datetime(frod_call['last_call_c'])
      frod_call['call_date'] = pd.to_datetime(frod_call['call_date'])
      frod_call['date'] = frod_call['last_call_c'].astype('datetime64').dt.date

      # Удаляем вылезшие точки с нулями в полях

      col_list = ['id_c', 'phone', 'last_queue_c', 'step_c', 'marker_c', 'marker', 
                  'town_c', 'region_c', 'dialog', 'inbound_call', 
                  'last_step', 'city', 'town', 'region', 'billsec']

      for col in col_list:
         
         frod_call[col] = frod_call[col].astype(str).apply(lambda x: x.replace('.0', '')) 

      # Находим записи, которые продублировались во время джойна и маркируем их единицей

      frod_call = frod_call.sort_values(['id_c', 'call_date'])

      frod_call['duplicates'] = frod_call.duplicated(['id_c', 'last_call_c'], keep='last').astype(int)
      frod_call['last_call_c'] = pd.to_datetime(frod_call['last_call_c'])

      # Определяем качество базы

      frod_call['region_c'] = frod_call['region_c'].astype(str)
      frod_call['ptv_c'] = frod_call['ptv_c'].astype(str)
      frod_call['region_c'] = frod_call.apply(lambda row: defs.quality(row['ptv_c'], row['region_c']), axis=1)

      # Джойним с качеством

      quality_df = pd.read_csv(quality_path)
      quality_df.rename(columns={'id' : 'region_c'}, inplace = True)
      quality_df['Качество города'] = quality_df['Качество города'].apply(defs.del_staple)

      frod_call = frod_call.merge(quality_df[['region_c', 'Качество города']], how = 'left', on = 'region_c')
      frod_call['region_c'] = frod_call['Качество города']

      # Джойним с шагами

      step_df = pd.read_csv(f'{step_path}{step_csv}')
      
      step_df.rename(columns={'step' : 'step_c',
                              'ochered' : 'dialog'}, inplace = True)
      
      step_df[['step_c', 'dialog']] = step_df[['step_c', 'dialog']].astype('str')

      result_df = frod_call.merge(step_df[['step_c', 'dialog', 'type_steps']], how = 'left', on = ['step_c', 'dialog'])
      result_df['type_steps'] = result_df['type_steps'].fillna(0).astype(int)
      result_df = result_df.fillna('')


      # Джойним с городами

      city_df = pd.read_csv(city_path)
      city_df['Область'] = city_df['Область'].apply(defs.find_letter)
      city_df = city_df.drop_duplicates('town_c', keep = 'first', inplace=False)
      city_df['town_c'] = city_df['town_c'].astype('str').apply(lambda x: x.replace('.0', '')) 

      result_df = result_df.merge(city_df[['town_c', 'Область']], how = 'left', on = 'town_c')
      result_df['town_c'] = result_df['Область']

      # Фильтруем ненужные поля и сохраняем датафрейм в csv

      result_df['request'] = result_df.apply(lambda row: defs.find_request(row['client_status'], row['duplicates']), axis = 1)

      result_df = result_df[['date', 'phone', 'last_queue_c', 'step_c', 'contacts_status_c', 'marker_c', 'ptv_c', 'town_c', 
         'region_c', 'last_call_c', 'call_date', 'dialog', 'inbound_call', 'route', 'last_step', 'client_status', 'city',
         'marker', 'town', 'ptv', 'region', 'type_steps', 'duplicates', 'request', 'region2', 'region2_c', 'billsec']]

      result_df.to_csv(f'{result_path}{frod_csv}', index = False)

   else:
      print('contact_df empty')
