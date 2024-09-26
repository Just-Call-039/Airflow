import pandas as pd
import glob
import os

def find_letter(city):
    city_new = ''
    for letter in city:
        
        if letter.isalpha() == True:
            
            city_new = ''.join([city_new, letter])
        
    return(city_new)

def region_defination(city, city_c, town):
    if city not in ['0', '', ' ']:
        return city
    elif city_c not in ['0', '', ' ']:
        return city_c
    else:
        return town
    
def area_defination(area, area_guess):
    if area == 0:
        return area_guess
    else:
        return area

def area_defination_str(area, area_guess):
    if area == '0':
        return area_guess
    else:
        return area

def download_files(path):
    i = 0
    for filename in os.listdir(path):
        if i == 0:
            df = pd.read_csv(f'{path}{filename}')
            i += 1
        else:
            df = pd.concat([df, pd.read_csv(f'{path}{filename}')], ignore_index = True, axis=0)
    return df

def del_point_zero(df, col_list):
    
    for col in col_list:
    
        df[col] = df[col].apply(lambda x: x.replace('.0', ''))

def update_project(project_x, project_y):
    if project_x == '':
        return project_y
    else:
        return project_x    

def fill_nan(x, y):

    if (x == 0) | (x == '0') | (x == '') | (x == ' '):
        return y
    else:
        return x    