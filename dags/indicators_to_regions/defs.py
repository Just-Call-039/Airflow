import pandas as pd
import glob
import os

def rtk_reg_r(row):
    if row['RTK_city'] not in ['', ' ']:
        return row['RTK_city']
    else:
        if row['RTK_city_c'] not in ['', ' ']:
            return row['RTK_city_c']
        else:
            return row['RTK_town']
        
def ttk_reg_r(row):
    if row['TTK_city'] not in ['', ' ']:
        return row['TTK_city']
    elif row['TTK_city_c'] not in ['', ' ']:
        return row['TTK_city_c']
    else:
        return row['TTK_town']


def mts_reg_r(row):
    if row['MTS_city'] not in ['', ' ']:
        return row['MTS_city']
    elif row['MTS_city_c'] not in ['', ' ']:
        return row['MTS_city_c']
    else:
        return row['MTS_town']


def bln_reg_r(row):
    if row['BLN_city'] not in ['', ' ']:
        return row['BLN_city']
    elif row['BLN_city_c'] not in ['', ' ']:
        return row['BLN_city_c']
    else:
        return row['BLN_town']
    
def rtk_reg(row):
    if row['RTK_city'] not in ['', ' ']:
        return row['RTK_city']
    else:
        if row['RTK_city_c'] not in ['', ' ']:
            return row['RTK_city_c']
        else:
            return row['RTK_town']

def ttk_reg(row):
    if row['TTK_city'] not in ['', ' ']:
        return row['TTK_city']
    elif row['TTK_city_c'] not in ['', ' ']:
        return row['TTK_city_c']
    else:
        return row['TTK_town']

def mts_reg(row):
    if row['MTS_city'] not in ['', ' ']:
        return row['MTS_city']
    elif row['MTS_city_c'] not in ['', ' ']:
        return row['MTS_city_c']
    else:
        return row['MTS_town']

def bln_reg(row):
    if row['BLN_city'] not in ['', ' ']:
        return row['BLN_city']
    elif row['BLN_city_c'] not in ['', ' ']:
        return row['BLN_city_c']
    else:
        return row['BLN_town']
