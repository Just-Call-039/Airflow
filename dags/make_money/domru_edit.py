def lids_editing (url,path_to_file_sql_airflow,csv_domru):
    import pandas as pd



    print(url)
    print('Начинаем читать файл с сайта')
    df = pd.read_csv(url, sep=';', encoding='utf-8').fillna('').astype('str')
    filtered_df = df[df['Технология подключения дома'].str.contains('FTTB|UTP')]
    filtered_df['Этажей'] = filtered_df['Этажей'].astype('str').apply(lambda x: x.replace('.0',''))
    filtered_df['Подъезды'] = filtered_df['Подъезды'].astype('str').apply(lambda x: x.replace('.0',''))
    filtered_df['Дом'] = filtered_df['Дом'].astype('str').apply(lambda x: x.replace('.0',''))
    filtered_df['Корпус'] = filtered_df['Корпус'].astype('str').apply(lambda x: x.replace('.0',''))
    filtered_df = filtered_df[['Город','Нас. пункт','Название улицы','Дом','Корпус','Дата подключения дома']]
    filtered_df['full'] = filtered_df.apply(lambda row: ';'.join([str(row['Нас. пункт']), str(row['Название улицы']), str(row['Дом']), str(row['Корпус'])]), axis=1)
    print('Сохраняем его')
    filtered_df.to_csv(rf'{path_to_file_sql_airflow}/{csv_domru}', index=False, sep=',', encoding='utf-8')