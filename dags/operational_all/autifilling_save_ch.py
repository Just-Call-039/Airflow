from commons_liza import to_click
import pandas as pd

def save_autofilling_ch(path_df, type_dict):

    client = to_click.my_connection()

    date_parse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

    df = pd.read_csv(path_df, dtype=type_dict, parse_dates = ['datetime', 'date_stop'], date_parser = date_parse)
  
    print(df.info())


    client.insert_dataframe('INSERT INTO autofilling VALUES', df)
    