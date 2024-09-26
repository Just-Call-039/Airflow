def excel_queues():
    import gspread
    import pandas as pd
    # from google.oauth2.service_account import Credentials
    from oauth2client.service_account import ServiceAccountCredentials

    # Настройки для аутентификации
    path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json' 
    table_name = 'Группировка очередей'

    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    to_excel = '/root/airflow/dags/project_defenition/projects/Группировка очередей.xlsx'

    credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
    gs = gspread.authorize(credentials)
    spreadsheet = gs.open(table_name)

    print('Создание нового Excel файла')
    with pd.ExcelWriter(to_excel, engine='openpyxl') as writer:
        print('Проходим по каждому листу в таблице')
        for sheet in spreadsheet.worksheets():
            
            print(sheet)
            print('Получаем данные из листа')
            if sheet.get_values() == []:
                pass
            else:
                values = sheet.get_values()  
                print('Сохраняем данные в DataFrame')
                df = pd.DataFrame(values[1:], columns=values[0])  # Первая строка используется как заголовок
                print('Записываем DataFrame в отдельный лист Excel')
                df.to_excel(writer, sheet_name=sheet.title, index=False)

    print("Данные успешно скачаны и сохранены в output.xlsx")
