from commons_liza import to_click
from time import sleep

def data_table(data_create, data_insert):

    print('Подключаемся к clickhouse')

    client = to_click.my_connection()
    cluster = '{cluster}'
    try:
        sql_drop = f'''TRUNCATE TABLE data ON CLUSTER '{cluster}' '''
        client.execute(sql_drop)
        sleep(600)
        print('Таблица очищена')
    except:
        print('Таблица не найдена')
    
    sql_create = open(data_create).read().replace('п»ї','').replace('﻿','').replace('\ufeff','')
    client.execute(sql_create)
    print('Таблица создана')

    print('Приводим к общему виду, и заливаем в таблицу')
    sql_insert = open(data_insert).read().replace('п»ї','').replace('﻿','').replace('\ufeff','')

    client.execute(sql_insert)