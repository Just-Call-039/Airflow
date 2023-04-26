def data_transformation(files_from_sql, files_to_csv):
    import pandas as pd
    from datetime import datetime
    # from tqdm import tqdm
    # tqdm.pandas()
    import glob
    import os
    import all_data.defs_redactor as redactor


    n = 0
    folder = os.listdir(files_from_sql)
    print(f'Всего файлов {len(folder)}')


    print('Начинаем основную часть')
    for _ in range(0,len(folder)):
        
        print('______________________________________________')
        print('Читаем файл')
        print(datetime.today().strftime("%m/%d/%Y, %H:%M:%S"))

        files = glob.glob(files_from_sql + "/*.csv")

        print(folder[n])
        chunk = pd.read_csv(files[n], sep=',')
        rows = chunk.shape[0]
        
        print('Обработка')

        print(' --- Разбираем ПТВ')
        i = 0
        for now in redactor.list_project:
            for position in range(len(redactor.list_ptv)):
                column_2 = f'{now}{redactor.list_ptv_reg[position]}'
                column = f'{redactor.list_project_reg[i]}{redactor.list_ptv_reg[position]}'
                phrase = f'^{now}{redactor.list_ptv[position]}^'
                chunk[column] = chunk['ptv_c'].apply(lambda x: 1 if phrase in str(x) else 0)
            i += 1
            
        print(' --- НТВ в ПТВ')
        chunk['ptv_n'] = chunk['ptv_c'].astype('str').apply(lambda x: 1 if any(w in x for w in ['^n^']) else 0)
        
            
        print(' -- Разбираем стоплисты')
        print(' --- По проектам')
        r = 0
        for now in redactor.list_project:
            for position in range(len(redactor.list_project_stop)):
                column_2 = f'{redactor.list_project_stop[position]}{now}'
                column = f'stop_{redactor.list_project_reg[r]}_{redactor.list_project_stop[position]}'
                phrase = f'^{redactor.list_project_stop[position]}{now}^'
                chunk[column] = chunk['stoplist_c'].apply(lambda x: 1 if phrase in str(x) else 0)
            r += 1
            
        print(' -- Общие')
        for now in redactor.list_stop:
            phrase = f'^{now}^'
            column = f'stop_{now}'
            chunk[column] = chunk['stoplist_c'].apply(lambda x: 1 if phrase in str(x) else 0)
            
        print(' --- Был на операторе')
        chunk['source_operator'] = chunk['base_source_c'].astype('str').apply(lambda x: 1 if any(w in x for w in redactor.list_source_operator) else 0)
        print(' --- Был на РО')
        chunk['source_ro'] = chunk['base_source_c'].astype('str').apply(lambda x: 1 if any(w in x for w in redactor.list_source_ro) else 0)
        print(' --- Отказники')
        chunk['source_otkazy_ro1'] = chunk['base_source_c'].astype('str').apply(lambda x: 1 if any(w in x for w in redactor.list_source_otkazy) else 0)
        chunk['source_otkazy_ro2'] = chunk['base_source_c'].astype('str').apply(lambda x: 1 if any(w in x for w in redactor.list_source_otkazy2) else 0)
        print(' --- Статусы')
        chunk['source_status'] = chunk['contacts_status_c'].astype('str').apply(lambda x: 1 if any(w in x for w in redactor.list_status) else 0)
        print(' --- Провайдеры')
        chunk['source_istochnik'] = chunk['istochnik_combo_c'].astype('str').apply(lambda x: 1 if any(w in x for w in redactor.list_istochnik) else 0)
        print(' --- Причины отказов')
        chunk['source_otkaz'] = chunk['otkaz_c'].astype('str').apply(lambda x: 1 if any(w in x for w in redactor.list_otkaz) else 0)
        
            
        print(' --- Применяем функции перевода region_c, network, phone')
        chunk = chunk.fillna(0)
        chunk['network_provider_c'] = chunk['network_provider_c'].astype('str').apply(lambda x: redactor.network_provider_c(x))
        chunk['region_c'] = chunk['region_c'].astype('str').apply(lambda x: redactor.region_c(x))
        chunk['phone'] = chunk['phone_work'].astype('str').apply(lambda x: redactor.phone(x))
        chunk['last_project'] = chunk['last_project'].astype('int')
        chunk['next_project'] = chunk['next_project'].astype('int')
        chunk['rtk_data'] = chunk['base_source_c'].astype('str').apply(lambda x: redactor.rtk_data(x))
        chunk['last_call'] = chunk['last_call'].astype('str').apply(lambda x: '1970-01-01' if x == '0' else x).apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))

        # print(' --- Соединяем с категориями из кликхайса')
        # chunk = chunk.merge(category, how='left', on = 'phone_work')

        print(' -- Группируем чанк')
        df_group = chunk.groupby([
            'region_c','network_provider_c',
            'town_c','city_c',
        'priority1','priority2','last_project','next_project','last_call',
            
        'bln_nasha','bln_sput','bln_telecom','bln_50','bln_50_40','bln_40_30','bln_30_20','bln_20_0',
        'mts_nasha','mts_sput','mts_telecom','mts_50','mts_50_40','mts_40_30','mts_30_20','mts_20_0',
        'nbn_nasha','nbn_sput','nbn_telecom','nbn_50','nbn_50_40','nbn_40_30','nbn_30_20','nbn_20_0',
        'dom_nasha','dom_sput','dom_telecom','dom_50','dom_50_40','dom_40_30','dom_30_20','dom_20_0',
        'rtk_nasha','rtk_sput','rtk_telecom','rtk_50','rtk_50_40','rtk_40_30','rtk_30_20','rtk_20_0',
        'ttk_nasha','ttk_sput','ttk_telecom','ttk_50','ttk_50_40','ttk_40_30','ttk_30_20','ttk_20_0',
            
        'stop_bln_s','stop_bln_c','stop_bln_cr','stop_bln_n','stop_bln_o',
        'stop_mts_s','stop_mts_c','stop_mts_cr','stop_mts_n','stop_mts_o',
        'stop_nbn_s','stop_nbn_c','stop_nbn_cr','stop_nbn_n','stop_nbn_o',
        'stop_dom_s','stop_dom_c','stop_dom_cr','stop_dom_n','stop_dom_o',
        'stop_rtk_s','stop_rtk_c','stop_rtk_cr','stop_rtk_n','stop_rtk_o',
        'stop_ttk_s','stop_ttk_c','stop_ttk_cr','stop_ttk_n','stop_ttk_o',
        'stop_s','stop_ao','stop_sb','stop_p','rtk_data',
        'source_operator','source_ro','phone','source_otkazy_ro1','source_otkazy_ro2',
        'source_status','source_istochnik','source_otkaz','ptv_n'
        # ,'category'
        ], as_index=False, dropna=False).agg({'phone_work': 'count'}) \
            .sort_values('phone_work').rename(columns={'phone_work': 'contacts'})
        
        print(' -- Считаем дни отдыха')
        df_group['rest_days'] = pd.Timestamp.now().normalize() - df_group['last_call']
        df_group['rest_days'] = df_group['rest_days'].astype('str').apply(lambda x: x.strip(' days')).astype('int64')
        
        print('Сохраняем файл')
        df_group.to_csv(f'{files_to_csv}/{folder[n]}', sep=',', index=False, encoding='utf-8')

        n += 1
        
        print('Завершение обработки')
        print(f'Текущий цикл {n}')
        print(f'Взято в работу строк {rows}')
        print(f'Контактов  после обработки {df_group.contacts.sum()}')
        print(datetime.today().strftime("%m/%d/%Y, %H:%M:%S"))


