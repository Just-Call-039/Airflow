import pandas as pd

# Оперделение проекта

def update_project(x, y):
    if x == '':
        return y
    else:
        return x
    

# Удаление '.0'
  
def del_point_zero(df, col_list):
    
    for col in col_list:
        print(col)
        df[col] = df[col].apply(lambda x: x.replace('.0', ''))

# Очистка названия городов и областей от цифр и скобок
def find_letter(city):
    
    if city != '':
        city = city[:-1]
        for letter in city:

            if letter.isalpha() != True:
                city = city[1:]
           
            else:
                return city       
    return city