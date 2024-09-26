import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def download_gs(table, sheet):

    path_to_credential = '/root/airflow/dags/quotas-338711-1e6d339f9a93.json'

    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
    gs = gspread.authorize(credentials)

    data = gs.open(table).worksheet(sheet).get_all_values()
    
    df = pd.DataFrame(data, columns = data.pop(0))
    print('download table: ', table, 'sheet: ', sheet, ' done')
    print('size: ', df.shape[0])

    return df