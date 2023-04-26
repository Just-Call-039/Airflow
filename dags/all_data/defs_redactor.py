
def network_provider_c(i):
    if i in {'10','68'}:
        return 'Теле 2'
    elif i == '80':
        return 'Билайн'
    elif i == '82':
        return 'Мегафон'
    elif i == '83':
        return 'МТС'
    else:
        return 'MVNO'
    
def region_c(x):
    for i in x:
        if i == '1':
            return 'Наша полная'
        elif i == '2':
            return 'Наша не полная'
        elif i == '3':
            return 'ПТВ в карте'
        elif i == '4':
            return 'Фиас из разных источников'
        elif i == '5':
            return 'Фиас до города'
        elif i == '6':
            return 'Старый town_c'
        elif i == '7':
            return 'Def-code'
        else:
            return '0'
        
def rtk_data(x):
    if '^180^' in x:
        return 'Январь РТК'
    elif '^173^' in x:
        return 'Декабрь РТК'
    elif '^172^' in x:
        return 'Ноябрь РТК'
    elif '^179^' in x:
        return 'Октябрь РТК'
    elif '^178^' in x:
        return 'Сентябрь РТК'
    else:
        return ''
    
def phone(x):
    if len(x) == 11:
        if x.startswith('89'):
            return 'Мобильный'
        elif x.startswith('8'):
            return 'Городской'
        else:
            return '0'
        
def group(x):    
    x = x.groupby([
         'region_c','network_provider_c','town_c','city_c',
         'priority1','priority2','last_project','next_project','last_call',
         'bln_nasha','bln_sput','bln_telecom','bln_50','bln_50_40','bln_40_30','bln_30_20','bln_20_0',
         'mts_nasha','mts_sput','mts_telecom','mts_50','mts_50_40','mts_40_30','mts_30_20','mts_20_0',
         'nbn_nasha','nbn_sput','nbn_telecom','nbn_50','nbn_50_40','nbn_40_30','nbn_30_20','nbn_20_0',
         'dom_nasha','dom_sput','dom_telecom','dom_50','dom_50_40','dom_40_30','dom_30_20','dom_20_0',
         'rtk_nasha','rtk_sput','rtk_telecom','rtk_50','rtk_50_40','rtk_40_30','rtk_30_20','rtk_20_0',
         'ttk_nasha','ttk_sput','ttk_telecom','ttk_50','ttk_50_40','ttk_40_30','ttk_30_20','ttk_20_0',
         'stop_bln_s','stop_bln_c','stop_bln_cr','stop_bln_n','stop_bln_o',
         'stop_mts_s','stop_mts_c','stop_mts_cr','stop_mts_n','stop_mts_o',
         'stop_nbn_s','stop_nbn_c','stop_nbn_cr','stop_nbn_n','stop_nbn_o',
         'stop_dom_s','stop_dom_c','stop_dom_cr','stop_dom_n','stop_dom_o',
         'stop_rtk_s','stop_rtk_c','stop_rtk_cr','stop_rtk_n','stop_rtk_o',
         'stop_ttk_s','stop_ttk_c','stop_ttk_cr','stop_ttk_n','stop_ttk_o',
         'stop_s','stop_ao','stop_sb','stop_p','rtk_data','phone',
         'source_otkazy_ro1','source_otkazy_ro2',
#         'source_otkazy',
         'source_status','source_istochnik','source_otkaz','ptv_n','category',
         'source_operator','source_ro','rest_days'],
                    as_index = False, dropna=False).agg({'contacts': 'sum'})
    return x

# Список ptv
list_ptv = ('', '_21', '_20', '_19', '_18', '_17', '_16', '_15')
list_ptv_reg = ('_nasha', '_sput', '_telecom', '_50', '_50_40', '_40_30', '_30_20', '_20_0')

# Список проектов
list_project = (10, 11, 19, 3, 5, 6)
list_project_reg = ('bln','mts','nbn','dom','rtk','ttk')

# # Список стоплистов
# Общие
list_stop = ('s','ao','sb','p')
# По проектам
list_project_stop = ('s','c','cr','n','o')

# # Список источников
# Был на операторе
list_source_operator = ['^121^','^122^','^140^','^119^','123^','^142^','^120^','^124^','^143^','^127^','^128^']
# Был на РО
list_source_ro = ['^117^','^125^','^126^','^144^','^145^','^146^','^147^']
# Отказники
list_source_otkazy = ['^131^','^132^']
list_source_otkazy2 = ['^152^','^153^']

# Стандартные стоплисты
list_status = ['MeetingWait', '0', '1','2'
#                ,'NoAnswer'
               ,'no_active']
list_istochnik = ['bank','ttkb2b','BeelineB2B','b2b','THO']
list_otkaz = ['otkaz_10','otkaz_8']