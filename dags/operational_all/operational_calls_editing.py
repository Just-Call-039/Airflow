def operational_calls_transformation(path_to_folder, name_calls, path_to_final_folder):
    import pandas as pd
    from operational_all.redactor_operational_calls import network
    from operational_all.redactor_operational_calls import data

    df = pd.read_csv(f'{path_to_folder}/{name_calls}')
    
    print('Переименование колонок')
    df = df.rename(columns={'пїЅпїЅпїЅпїЅ': 'База',
            'пїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅ': 'Разговоры',
            'пїЅпїЅпїЅпїЅпїЅпїЅ': 'Звонки',
            'пїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅ': 'Переводы',
            'пїЅпїЅпїЅпїЅпїЅпїЅ.1': 'Заявки',
            'Р\xa0Р°Р·РіРѕРІРѕСЂС‹': 'Разговоры',
            'Р—РІРѕРЅРєРё': 'Звонки',
            'РџРµСЂРµРІРѕРґС‹': 'Переводы',
            'Р—Р°СЏРІРєРё': 'Заявки',
            'Р‘Р°Р·Р°': 'База'})
    
    print('Изменение типа колонки')
    df['Разговоры'] = df['Разговоры'].fillna(0).astype('int')
#     df['Звонки'] = df['Звонки'].fillna(0).astype('int')
#     df['Переводы'] = df['Переводы'].fillna(0).astype('int')
#     df['Заявки'] = df['Заявки'].fillna(0).astype('int')

    df['network_provider'] = df.apply(lambda row: network(row), axis=1)
    df['База'] = df.apply(lambda row: data(row), axis=1)

    print('Сохраняем')
    df.to_csv(f'{path_to_final_folder}/{name_calls}', sep=',', index=False, encoding='utf-8')

operational_calls_transformation(path_to_folder = '/root/airflow/dags/operational_all/Files/sql_operational/',
                                 name_calls = 'operational_calls.csv',
                                 path_to_final_folder = '/root/airflow/dags/operational_all/Files/operational/'
                                )