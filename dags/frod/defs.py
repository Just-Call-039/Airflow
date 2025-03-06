import pandas as pd

# Функция определяет звонок был после фродилки или нет

def diff_time(x, y):

    if y != 0:
        if x < y:
            return 1
        else:
            return 0
    else:
        return 0
    
# Удаляет точку с нулем в полях
        
def del_zero(df, col_list):
    
    for col in col_list:
        
        df[col] = df[col].fillna('0').astype(str).apply(lambda x: x.replace('.0', ''))

# Удаление скобок в названии городов

def find_letter(city):
    
    city_new = ''
    for letter in city:
        
        if letter.isalpha() == True:
            
            city_new = ''.join([city_new, letter])
        
    return(city_new)

# Определение нашей базы

def quality(ptv, region):
    
    ptv_nasha = ['^5^', '^6^', '^3^', '^10^', '^11^', '^19^']
    ptv_ne_nasha = ['^5_15^', '^5_16^', '^5_17^', '5_18^', '^5_19^', '^5_20^', '^5_21^',
                '^6_15^', '^6_16^', '^6_17^', '6_18^', '^6_19^', '^6_20^', '^6_21^',
                '^3_15^', '^3_16^', '^3_17^', '3_18^', '^3_19^', '^3_20^', '^3_21^',
                '^10_15^', '^10_16^', '^10_17^', '10_18^', '^10_19^', '^10_20^', '^10_21^',
                '^11_15^', '^11_16^', '^11_17^', '11_18^', '^11_19^', '^11_20^', '^11_21^',
                '^19_15^', '^19_16^', '^19_17^', '19_18^', '^19_19^', '^19_20^', '^19_21^']

    if any(w in ptv for w in ptv_nasha):
        return 'ptv_1'
    elif any(w in ptv for w in ptv_ne_nasha):
        return 'ptv_2'
    else:
        return region
    
def find_request(status, duplicates):

    if (duplicates == 0) & (status == 'MeetingWait'):
        return 1
    else:
        return 0
    
def del_staple(x):
    
    if x != 'Пусто':
        x = x[1:]
        x = x[:-1]
    
    return x

 