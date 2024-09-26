# склеивает файлике с лидами
import pandas as pd
from clickhouse_driver import Client
import glob
import os
import datetime
        
#all_files = len(os.listdir("/root/airflow/dags/project_defenition/projects/teams/*.csv"))
#print(f'Всего файлов {all_files}')

"""csv_files = glob.glob("/root/airflow/dags/project_defenition/projects/teams/*.csv")
df_lidi = pd.DataFrame()




today = datetime.date.today().day -1
print(f'Лиды будут собраны с последних {today} файлов')
csv_files = sorted(csv_files, reverse=False)
csv_files = csv_files[-today:]

print(len(csv_files)) # = csv_files[]

for file in csv_files:
    df_lidi = pd.concat([df_lidi, pd.read_csv(file)], ignore_index=True)

print(len(df_lidi))
print(csv_files)
df_lidi.to_csv('/root/airflow/dags/report_10/report_files/Лиды.csv', index=False)

print('Таблица сохранена')"""


# -------------------- 
"""print('\n\n\nПроверка загрузки последнего файла')

csv_files = glob.glob("/root/airflow/dags/project_defenition/projects/teams/*.csv")
csv_files = sorted(csv_files, reverse=False)
csv_files = csv_files[-1]
print('файл Лиды', csv_files)"""
# -------------------- 

print('\n\n\nПроверка загрузки файллов за текущий месяц оп вчерашнее число + собирает файл')
csv_files = glob.glob("/root/airflow/dags/project_defenition/projects/teams/*.csv")
today = datetime.date.today().day -1
csv_files = sorted(csv_files, reverse=False)
csv_files = csv_files[-today:]
df_lidi = pd.DataFrame()
for i in csv_files:
    print(i)
for file in csv_files:
    df_lidi = pd.concat([df_lidi, pd.read_csv(file)], ignore_index=True)
df_lidi.to_csv('Лиды.csv', index=False)
df_lidi.to_csv('/root/airflow/dags/report_10/report_files/Лиды.csv', index=False)
print('\nФайл Лиды сохранен. всего записей: ', len(df_lidi))
# -------------------- 
"""print('\n\n\nПроверка загрузки файллов за прошлый месяц')
csv_files = glob.glob("/root/airflow/dags/project_defenition/projects/teams/*.csv")
today = datetime.date.today().day -1
csv_files = sorted(csv_files, reverse=False)
csv_files = csv_files[-(today+31):-today]

for i in csv_files:
    print(i)
"""

